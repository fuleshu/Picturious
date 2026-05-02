#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use anyhow::Context;
use picturious_core::{
    FolderSummary, FolderView, FolderViewHeader, FolderViewTarget, GeneratedThumbnail,
    ImageSummary, LibraryManager, LibraryOverview, LibraryRoot, RootDatabase,
    RotationDirection as CoreRotationDirection, ScanReport, ScanTarget, ThumbnailCache,
    ThumbnailResponse, generate_thumbnail, rotate_image as rotate_image_file,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tauri::{
    AppHandle, Emitter, Manager, Monitor, PhysicalPosition, PhysicalSize, Position, Size, State,
    WebviewWindow, Window, WindowEvent,
};

struct AppState {
    library: Arc<Mutex<LibraryManager>>,
    thumbnails: Arc<Mutex<ThumbnailCache>>,
    active_scans: Arc<Mutex<HashSet<String>>>,
    settings: Arc<Mutex<UiSettings>>,
    settings_path: Arc<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UiSettings {
    #[serde(default = "default_thumb_scale")]
    thumb_scale: f64,
    #[serde(default)]
    upscale_fullscreen_images: bool,
    #[serde(default = "default_slideshow_speed_seconds")]
    slideshow_speed_seconds: f64,
    #[serde(default)]
    slideshow_loop: bool,
    #[serde(default)]
    slideshow_ignore_smaller_than: u32,
    #[serde(default)]
    external_viewers: Vec<ExternalViewer>,
    #[serde(default)]
    window: Option<WindowSettings>,
}

impl Default for UiSettings {
    fn default() -> Self {
        Self {
            thumb_scale: default_thumb_scale(),
            upscale_fullscreen_images: false,
            slideshow_speed_seconds: default_slideshow_speed_seconds(),
            slideshow_loop: false,
            slideshow_ignore_smaller_than: 0,
            external_viewers: Vec::new(),
            window: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ExternalViewer {
    id: String,
    name: String,
    path: String,
}

#[derive(Debug, Clone, Deserialize)]
struct UiPreferences {
    #[serde(default)]
    upscale_fullscreen_images: bool,
    #[serde(default = "default_slideshow_speed_seconds")]
    slideshow_speed_seconds: f64,
    #[serde(default)]
    slideshow_loop: bool,
    #[serde(default)]
    slideshow_ignore_smaller_than: u32,
    #[serde(default)]
    external_viewers: Vec<ExternalViewer>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WindowSettings {
    x: i32,
    y: i32,
    width: u32,
    height: u32,
}

#[derive(Clone, Serialize)]
struct ScanError {
    root_id: String,
    message: String,
}

#[derive(Clone, Serialize)]
struct FolderValidated {
    root_id: String,
    relative_path: String,
    changed: bool,
}

#[derive(Clone, Serialize)]
struct FolderViewStarted {
    request_id: u64,
    view: FolderViewHeader,
}

#[derive(Clone, Serialize)]
struct FolderViewBatch {
    request_id: u64,
    folders: Vec<FolderSummary>,
    images: Vec<ImageSummary>,
}

#[derive(Clone, Serialize)]
struct FolderViewFinished {
    request_id: u64,
    root_id: String,
    relative_path: String,
    folder_count: u32,
    image_count: u32,
}

#[derive(Clone, Serialize)]
struct FolderViewError {
    request_id: u64,
    root_id: String,
    relative_path: String,
    message: String,
}

#[derive(Clone, Serialize)]
struct FolderValidationFinished {
    request_id: u64,
    root_id: String,
    changed_paths: Vec<String>,
}

#[derive(Clone, Serialize)]
struct FolderValidationError {
    request_id: u64,
    root_id: String,
    message: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RotationDirection {
    Left,
    Right,
}

impl From<RotationDirection> for CoreRotationDirection {
    fn from(direction: RotationDirection) -> Self {
        match direction {
            RotationDirection::Left => CoreRotationDirection::Left,
            RotationDirection::Right => CoreRotationDirection::Right,
        }
    }
}

#[tauri::command]
fn app_settings(state: State<'_, AppState>) -> Result<UiSettings, String> {
    state
        .settings
        .lock()
        .map(|settings| settings.clone())
        .map_err(|_| "app settings are locked".to_owned())
}

#[tauri::command]
fn save_thumb_scale(thumb_scale: f64, state: State<'_, AppState>) -> Result<(), String> {
    let mut settings = state
        .settings
        .lock()
        .map_err(|_| "app settings are locked".to_owned())?;
    settings.thumb_scale = clamp_thumb_scale(thumb_scale);
    sanitize_ui_settings(&mut settings);
    write_ui_settings(state.settings_path.as_ref().as_path(), &settings).map_err(error_message)
}

#[tauri::command]
fn save_app_preferences(
    preferences: UiPreferences,
    state: State<'_, AppState>,
) -> Result<UiSettings, String> {
    let mut settings = state
        .settings
        .lock()
        .map_err(|_| "app settings are locked".to_owned())?;
    settings.upscale_fullscreen_images = preferences.upscale_fullscreen_images;
    settings.slideshow_speed_seconds = preferences.slideshow_speed_seconds;
    settings.slideshow_loop = preferences.slideshow_loop;
    settings.slideshow_ignore_smaller_than = preferences.slideshow_ignore_smaller_than;
    settings.external_viewers = preferences.external_viewers;
    sanitize_ui_settings(&mut settings);
    write_ui_settings(state.settings_path.as_ref().as_path(), &settings).map_err(error_message)?;
    Ok(settings.clone())
}

#[tauri::command]
async fn library_overview(state: State<'_, AppState>) -> Result<LibraryOverview, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .overview()
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
fn pick_root_folder() -> Option<String> {
    rfd::FileDialog::new()
        .set_title("Choose Picture Root")
        .pick_folder()
        .map(|path| path.to_string_lossy().to_string())
}

#[tauri::command]
fn pick_external_viewer() -> Option<ExternalViewer> {
    let mut dialog = rfd::FileDialog::new()
        .set_title("Choose External Viewer")
        .add_filter("Programs", &["exe", "cmd", "bat"]);
    if let Some(program_files) = std::env::var_os("ProgramFiles") {
        dialog = dialog.set_directory(PathBuf::from(program_files));
    }

    dialog
        .pick_file()
        .map(|path| external_viewer_for_path(&path))
}

#[tauri::command]
async fn add_root(path: String, state: State<'_, AppState>) -> Result<LibraryRoot, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .add_root(&path)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn remove_root(
    root_id: String,
    state: State<'_, AppState>,
) -> Result<LibraryOverview, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        let mut library = library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?;
        library.remove_root(&root_id).map_err(error_message)?;
        library.overview().map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn start_scan(
    root_id: String,
    relative_path: String,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<bool, String> {
    let library = state.library.clone();
    let target = tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .scan_target(&root_id, &relative_path)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())??;

    {
        let mut active_scans = state
            .active_scans
            .lock()
            .map_err(|_| "scan state is locked".to_owned())?;
        if !active_scans.insert(target.root_id.clone()) {
            return Ok(false);
        }
    }

    let active_scans = state.active_scans.clone();
    let _ = tauri::async_runtime::spawn_blocking(move || {
        let event_root_id = target.root_id.clone();
        let result = run_scan(target, &app);

        if let Ok(mut active_scans) = active_scans.lock() {
            active_scans.remove(&event_root_id);
        }

        match result {
            Ok(report) => {
                let _ = app.emit("scan-finished", &report);
            }
            Err(error) => {
                let _ = app.emit(
                    "scan-error",
                    &ScanError {
                        root_id: event_root_id.clone(),
                        message: error.to_string(),
                    },
                );
            }
        }
    });

    Ok(true)
}

#[tauri::command]
async fn folder_view(
    root_id: String,
    relative_path: String,
    state: State<'_, AppState>,
) -> Result<FolderView, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .folder_view(&root_id, &relative_path)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn recursive_folder_images(
    root_id: String,
    relative_path: String,
    state: State<'_, AppState>,
) -> Result<Vec<ImageSummary>, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .recursive_images_for_folder(&root_id, &relative_path)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn stream_folder_view(
    root_id: String,
    relative_path: String,
    request_id: u64,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let library = state.library.clone();
    let target_root_id = root_id.clone();
    let target = tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .folder_view_target(&target_root_id)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())??;

    let error_root_id = root_id.clone();
    let error_relative_path = relative_path.clone();
    let _ = tauri::async_runtime::spawn_blocking(move || {
        if let Err(error) = stream_folder_view_for_target(target, relative_path, request_id, &app) {
            let message = error_message(error);
            let _ = app.emit(
                "folder-view-error",
                &FolderViewError {
                    request_id,
                    root_id: error_root_id.clone(),
                    relative_path: error_relative_path.clone(),
                    message,
                },
            );
        }
    });

    Ok(())
}

#[tauri::command]
async fn validate_folder_view(
    root_id: String,
    relative_path: String,
    visible_relative_paths: Vec<String>,
    request_id: u64,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let library = state.library.clone();
    let target_root_id = root_id.clone();
    let target = tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .folder_view_target(&target_root_id)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())??;

    let _ = tauri::async_runtime::spawn_blocking(move || {
        match validate_folder_view_for_target(target, relative_path, visible_relative_paths) {
            Ok(changed_paths) => {
                let _ = app.emit(
                    "folder-validation-finished",
                    &FolderValidationFinished {
                        request_id,
                        root_id,
                        changed_paths,
                    },
                );
            }
            Err(error) => {
                let _ = app.emit(
                    "folder-validation-error",
                    &FolderValidationError {
                        request_id,
                        root_id,
                        message: error_message(error),
                    },
                );
            }
        }
    });

    Ok(())
}

#[tauri::command]
async fn thumbnail(
    root_id: String,
    image_id: i64,
    size: u32,
    state: State<'_, AppState>,
) -> Result<ThumbnailResponse, String> {
    if root_is_scanning(&state.active_scans, &root_id)? {
        return Err("thumbnail generation is paused while scanning".to_owned());
    }

    let library = state.library.clone();
    let thumbnails = state.thumbnails.clone();
    let root_id_for_path = root_id.clone();
    let (path, modified_unix_ms) = tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .image_path(&root_id_for_path, image_id)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())??;

    if let Some(response) = thumbnails
        .lock()
        .map_err(|_| "thumbnail cache is locked".to_owned())?
        .get(image_id, &path, modified_unix_ms, size)
    {
        return Ok(response);
    }

    if root_is_scanning(&state.active_scans, &root_id)? {
        return Err("thumbnail generation is paused while scanning".to_owned());
    }

    let generated = spawn_thumbnail_job(path.clone(), size).await?;
    let response = generated.response(image_id, false);

    let mut cache = thumbnails
        .lock()
        .map_err(|_| "thumbnail cache is locked".to_owned())?;
    if let Some(response) = cache.get(image_id, &path, modified_unix_ms, size) {
        return Ok(response);
    }
    cache.insert_generated(&path, modified_unix_ms, size, generated);
    Ok(response)
}

#[tauri::command]
fn image_file_path(
    root_id: String,
    image_id: i64,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<String, String> {
    let (path, _) = state
        .library
        .lock()
        .map_err(|_| "library state is locked".to_owned())?
        .image_path(&root_id, image_id)
        .map_err(error_message)?;

    app.asset_protocol_scope()
        .allow_file(&path)
        .map_err(|error| error.to_string())?;

    Ok(path
        .to_string_lossy()
        .trim_start_matches(r"\\?\")
        .to_owned())
}

#[tauri::command]
fn set_viewer_fullscreen(fullscreen: bool, window: Window) -> Result<(), String> {
    if fullscreen {
        window.set_focus().map_err(|error| error.to_string())?;
    }
    window
        .set_fullscreen(fullscreen)
        .map_err(|error| error.to_string())?;
    if fullscreen {
        window.set_focus().map_err(|error| error.to_string())?;
    }
    Ok(())
}

#[tauri::command]
async fn rotate_image(
    root_id: String,
    image_id: i64,
    direction: RotationDirection,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let library = state.library.clone();
    let (path, _) = image_path_for(&library, &root_id, image_id)?;
    tauri::async_runtime::spawn_blocking(move || rotate_image_file(&path, direction.into()))
        .await
        .map_err(|error| error.to_string())?
        .map_err(error_message)?;

    library
        .lock()
        .map_err(|_| "library state is locked".to_owned())?
        .refresh_image_metadata(&root_id, image_id)
        .map_err(error_message)
}

#[tauri::command]
fn show_image_in_explorer(
    root_id: String,
    image_id: i64,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let (path, _) = image_path_for(&state.library, &root_id, image_id)?;
    show_in_explorer(&path).map_err(error_message)
}

#[tauri::command]
fn open_image_with(
    root_id: String,
    image_id: i64,
    viewer_id: String,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let viewer = state
        .settings
        .lock()
        .map_err(|_| "app settings are locked".to_owned())?
        .external_viewers
        .iter()
        .find(|viewer| viewer.id == viewer_id)
        .cloned()
        .ok_or_else(|| "external viewer is not configured".to_owned())?;
    let (path, _) = image_path_for(&state.library, &root_id, image_id)?;
    open_with_external_viewer(&viewer, &path).map_err(error_message)
}

#[tauri::command]
async fn move_image_to_recycle_bin(
    root_id: String,
    image_id: i64,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let library = state.library.clone();
    let (path, _) = image_path_for(&library, &root_id, image_id)?;
    tauri::async_runtime::spawn_blocking(move || recycle_file(&path))
        .await
        .map_err(|error| error.to_string())?
        .map_err(error_message)?;

    library
        .lock()
        .map_err(|_| "library state is locked".to_owned())?
        .delete_image(&root_id, image_id)
        .map_err(error_message)
}

#[tauri::command]
fn set_folder_thumbnail(
    root_id: String,
    folder_id: i64,
    image_id: i64,
    state: State<'_, AppState>,
) -> Result<(), String> {
    state
        .library
        .lock()
        .map_err(|_| "library state is locked".to_owned())?
        .set_folder_thumbnail(&root_id, folder_id, image_id)
        .map_err(error_message)
}

fn main() {
    tauri::Builder::default()
        .setup(|app| {
            let config_dir = app.path().app_config_dir()?;
            let settings_dir = app.path().app_local_data_dir()?;
            let settings_path = Arc::new(settings_dir.join("settings.json"));
            let settings = Arc::new(Mutex::new(read_ui_settings(settings_path.as_ref())));
            app.manage(AppState {
                library: Arc::new(Mutex::new(LibraryManager::new(&config_dir)?)),
                thumbnails: Arc::new(Mutex::new(ThumbnailCache::default())),
                active_scans: Arc::new(Mutex::new(HashSet::new())),
                settings: settings.clone(),
                settings_path: settings_path.clone(),
            });
            if let Some(window) = app.get_webview_window("main") {
                if let Ok(settings_snapshot) = settings.lock().map(|settings| settings.clone()) {
                    let _ = restore_window_state(&window, &settings_snapshot);
                }
                track_window_state(&window, settings, settings_path);
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            app_settings,
            save_thumb_scale,
            save_app_preferences,
            library_overview,
            pick_root_folder,
            pick_external_viewer,
            add_root,
            remove_root,
            start_scan,
            folder_view,
            recursive_folder_images,
            stream_folder_view,
            validate_folder_view,
            thumbnail,
            image_file_path,
            set_viewer_fullscreen,
            rotate_image,
            show_image_in_explorer,
            open_image_with,
            move_image_to_recycle_bin,
            set_folder_thumbnail
        ])
        .run(tauri::generate_context!())
        .expect("failed to run Picturious");
}

fn external_viewer_for_path(path: &Path) -> ExternalViewer {
    let clean_path = path
        .to_string_lossy()
        .trim_start_matches(r"\\?\")
        .to_owned();
    let name = path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("External viewer")
        .to_owned();

    ExternalViewer {
        id: clean_path.clone(),
        name,
        path: clean_path,
    }
}

fn run_scan(target: ScanTarget, app: &AppHandle) -> anyhow::Result<ScanReport> {
    let mut db = RootDatabase::open(&target.path)?;
    let database_root_id = db.root_id()?;
    if database_root_id != target.root_id {
        anyhow::bail!(
            "root database id does not match the configured root id for {}",
            target.path.display()
        );
    }

    let mut last_emit = Instant::now() - Duration::from_millis(500);
    let mut last_folder_count = 0_u32;
    db.rescan_with_progress(&target.root_id, &target.relative_path, |progress| {
        if progress.changed || progress.folders_seen == 1 {
            let _ = app.emit(
                "folder-validated",
                &FolderValidated {
                    root_id: progress.root_id.clone(),
                    relative_path: progress.current_relative_path.clone(),
                    changed: progress.changed,
                },
            );
        }
        let enough_time = last_emit.elapsed() >= Duration::from_millis(500);
        let enough_work = progress.folders_seen.saturating_sub(last_folder_count) >= 500;
        if progress.folders_seen == 1 || enough_time || enough_work {
            let _ = app.emit("scan-progress", &progress);
            last_emit = Instant::now();
            last_folder_count = progress.folders_seen;
        }
    })
}

fn stream_folder_view_for_target(
    target: FolderViewTarget,
    relative_path: String,
    request_id: u64,
    app: &AppHandle,
) -> anyhow::Result<()> {
    let db = RootDatabase::open_existing(&target.path)?
        .with_context(|| format!("root database is missing: {}", target.path.display()))?;
    let database_root_id = db.root_id()?;
    if database_root_id != target.root_id {
        anyhow::bail!(
            "root database id does not match the configured root id for {}",
            target.path.display()
        );
    }

    let header = db.folder_view_header(&target.root_id, &target.display_name, &relative_path)?;
    app.emit(
        "folder-view-started",
        &FolderViewStarted {
            request_id,
            view: header.clone(),
        },
    )?;

    let mut folder_count = 0_u32;
    let mut image_count = 0_u32;
    db.stream_folder_view_items(&target.root_id, &header.relative_path, |folders, images| {
        folder_count = folder_count.saturating_add(folders.len() as u32);
        image_count = image_count.saturating_add(images.len() as u32);
        app.emit(
            "folder-view-batch",
            &FolderViewBatch {
                request_id,
                folders,
                images,
            },
        )?;
        Ok(())
    })?;

    app.emit(
        "folder-view-finished",
        &FolderViewFinished {
            request_id,
            root_id: target.root_id,
            relative_path: header.relative_path,
            folder_count,
            image_count,
        },
    )?;
    Ok(())
}

fn validate_folder_view_for_target(
    target: FolderViewTarget,
    relative_path: String,
    visible_relative_paths: Vec<String>,
) -> anyhow::Result<Vec<String>> {
    let mut db = RootDatabase::open_existing(&target.path)?
        .with_context(|| format!("root database is missing: {}", target.path.display()))?;
    let database_root_id = db.root_id()?;
    if database_root_id != target.root_id {
        anyhow::bail!(
            "root database id does not match the configured root id for {}",
            target.path.display()
        );
    }

    let mut seen_paths = HashSet::new();
    let mut changed_paths = Vec::new();
    for path in std::iter::once(relative_path).chain(visible_relative_paths.into_iter()) {
        let path = path.replace('\\', "/").trim_matches('/').to_owned();
        if !seen_paths.insert(path.clone()) {
            continue;
        }

        if db.validate_folder_shallow(&target.root_id, &path)? {
            changed_paths.push(path);
        }
    }

    Ok(changed_paths)
}

fn error_message(error: anyhow::Error) -> String {
    error
        .chain()
        .map(|cause| cause.to_string())
        .collect::<Vec<_>>()
        .join(": ")
}

fn root_is_scanning(
    active_scans: &Arc<Mutex<HashSet<String>>>,
    root_id: &str,
) -> Result<bool, String> {
    active_scans
        .lock()
        .map(|active_scans| active_scans.contains(root_id))
        .map_err(|_| "scan state is locked".to_owned())
}

fn default_thumb_scale() -> f64 {
    1.0
}

fn default_slideshow_speed_seconds() -> f64 {
    3.0
}

fn clamp_thumb_scale(value: f64) -> f64 {
    if value.is_finite() {
        value.clamp(0.5, 2.0)
    } else {
        default_thumb_scale()
    }
}

fn normalize_slideshow_speed_seconds(value: f64) -> f64 {
    if value.is_finite() && value > 0.0 {
        (value * 1000.0).round() / 1000.0
    } else {
        default_slideshow_speed_seconds()
    }
}

fn normalize_slideshow_ignore_smaller_than(value: u32) -> u32 {
    match value {
        512 | 800 | 1024 => value,
        _ => 0,
    }
}

fn sanitize_ui_settings(settings: &mut UiSettings) {
    settings.thumb_scale = clamp_thumb_scale(settings.thumb_scale);
    settings.slideshow_speed_seconds =
        normalize_slideshow_speed_seconds(settings.slideshow_speed_seconds);
    settings.slideshow_ignore_smaller_than =
        normalize_slideshow_ignore_smaller_than(settings.slideshow_ignore_smaller_than);
    settings.external_viewers.retain(|viewer| {
        let path = Path::new(&viewer.path);
        path.is_file() && is_external_viewer_path(path)
    });
    for viewer in &mut settings.external_viewers {
        let path = PathBuf::from(&viewer.path);
        let clean_path = path
            .to_string_lossy()
            .trim_start_matches(r"\\?\")
            .to_owned();
        let fallback_name = path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("External viewer")
            .to_owned();
        viewer.id = clean_path.clone();
        viewer.path = clean_path;
        if viewer.name.trim().is_empty() {
            viewer.name = fallback_name;
        }
    }
    let mut seen_paths = HashSet::new();
    settings
        .external_viewers
        .retain(|viewer| seen_paths.insert(viewer.path.to_lowercase()));
}

fn read_ui_settings(path: &Path) -> UiSettings {
    let Some(contents) = path
        .is_file()
        .then(|| fs::read_to_string(path).ok())
        .flatten()
    else {
        return UiSettings::default();
    };

    let mut settings = serde_json::from_str::<UiSettings>(&contents).unwrap_or_default();
    sanitize_ui_settings(&mut settings);
    settings
}

fn write_ui_settings(path: &Path, settings: &UiSettings) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("could not create {}", parent.display()))?;
    }
    let json = serde_json::to_string_pretty(settings)?;
    fs::write(path, json).with_context(|| format!("could not write {}", path.display()))?;
    Ok(())
}

fn restore_window_state(window: &WebviewWindow, settings: &UiSettings) -> anyhow::Result<()> {
    let Some(saved) = settings.window.as_ref() else {
        return Ok(());
    };
    let restored = WindowSettings {
        x: saved.x,
        y: saved.y,
        width: saved.width.clamp(640, 10_000),
        height: saved.height.clamp(480, 10_000),
    };

    let monitors = window.available_monitors()?;
    if !window_rect_visible(&restored, &monitors) {
        return Ok(());
    }

    window.set_size(Size::Physical(PhysicalSize::new(
        restored.width,
        restored.height,
    )))?;
    window.set_position(Position::Physical(PhysicalPosition::new(
        restored.x, restored.y,
    )))?;
    Ok(())
}

fn track_window_state(
    window: &WebviewWindow,
    settings: Arc<Mutex<UiSettings>>,
    settings_path: Arc<PathBuf>,
) {
    let tracked_window = window.clone();
    window.on_window_event(move |event| {
        if matches!(event, WindowEvent::Moved(_) | WindowEvent::Resized(_)) {
            let _ = save_current_window_state(
                &tracked_window,
                &settings,
                settings_path.as_ref().as_path(),
            );
        }
    });
}

fn save_current_window_state(
    window: &WebviewWindow,
    settings: &Arc<Mutex<UiSettings>>,
    settings_path: &Path,
) -> anyhow::Result<()> {
    if window.is_fullscreen().unwrap_or(false) {
        return Ok(());
    }

    let position = window.outer_position()?;
    let size = window.outer_size()?;
    if size.width < 320 || size.height < 240 {
        return Ok(());
    }

    let mut settings = settings
        .lock()
        .map_err(|_| anyhow::anyhow!("app settings are locked"))?;
    settings.window = Some(WindowSettings {
        x: position.x,
        y: position.y,
        width: size.width,
        height: size.height,
    });
    write_ui_settings(settings_path, &settings)
}

fn window_rect_visible(window: &WindowSettings, monitors: &[Monitor]) -> bool {
    let window_left = i64::from(window.x);
    let window_top = i64::from(window.y);
    let window_right = window_left + i64::from(window.width);
    let window_bottom = window_top + i64::from(window.height);

    monitors.iter().any(|monitor| {
        let position = monitor.position();
        let size = monitor.size();
        let monitor_left = i64::from(position.x);
        let monitor_top = i64::from(position.y);
        let monitor_right = monitor_left + i64::from(size.width);
        let monitor_bottom = monitor_top + i64::from(size.height);

        let intersection_width = window_right.min(monitor_right) - window_left.max(monitor_left);
        let intersection_height = window_bottom.min(monitor_bottom) - window_top.max(monitor_top);
        intersection_width >= 80 && intersection_height >= 80
    })
}

fn image_path_for(
    library: &Arc<Mutex<LibraryManager>>,
    root_id: &str,
    image_id: i64,
) -> Result<(PathBuf, i64), String> {
    library
        .lock()
        .map_err(|_| "library state is locked".to_owned())?
        .image_path(root_id, image_id)
        .map_err(error_message)
}

fn show_in_explorer(path: &Path) -> anyhow::Result<()> {
    Command::new("explorer.exe")
        .arg(format!("/select,{}", path.display()))
        .spawn()
        .with_context(|| format!("could not open Explorer for {}", path.display()))?;
    Ok(())
}

fn is_external_viewer_path(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            ["exe", "cmd", "bat"]
                .iter()
                .any(|supported| extension.eq_ignore_ascii_case(supported))
        })
        .unwrap_or(false)
}

fn open_with_external_viewer(viewer: &ExternalViewer, image_path: &Path) -> anyhow::Result<()> {
    let viewer_path = PathBuf::from(&viewer.path);
    if !viewer_path.is_file() || !is_external_viewer_path(&viewer_path) {
        anyhow::bail!("external viewer is not available: {}", viewer.path);
    }

    let extension = viewer_path
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or_default();
    if extension.eq_ignore_ascii_case("cmd") || extension.eq_ignore_ascii_case("bat") {
        Command::new("cmd.exe")
            .arg("/C")
            .arg("start")
            .arg("")
            .arg(&viewer_path)
            .arg(image_path)
            .spawn()
            .with_context(|| format!("could not open {}", image_path.display()))?;
    } else {
        Command::new(&viewer_path)
            .arg(image_path)
            .spawn()
            .with_context(|| {
                format!(
                    "could not open {} with {}",
                    image_path.display(),
                    viewer_path.display()
                )
            })?;
    }

    Ok(())
}

#[cfg(windows)]
fn recycle_file(path: &Path) -> anyhow::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::UI::Shell::{
        FO_DELETE, FOF_ALLOWUNDO, FOF_NOCONFIRMATION, FOF_NOERRORUI, FOF_SILENT, SHFILEOPSTRUCTW,
        SHFileOperationW,
    };

    let mut from = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let mut operation = SHFILEOPSTRUCTW {
        wFunc: FO_DELETE,
        pFrom: from.as_mut_ptr(),
        fFlags: (FOF_ALLOWUNDO | FOF_NOCONFIRMATION | FOF_NOERRORUI | FOF_SILENT) as u16,
        ..Default::default()
    };

    let result = unsafe { SHFileOperationW(&mut operation) };
    if result != 0 {
        anyhow::bail!(
            "could not move {} to the recycle bin (shell error {result})",
            path.display()
        );
    }
    if operation.fAnyOperationsAborted != 0 {
        anyhow::bail!("move to recycle bin was canceled");
    }
    Ok(())
}

#[cfg(not(windows))]
fn recycle_file(path: &Path) -> anyhow::Result<()> {
    anyhow::bail!(
        "recycle bin is only implemented on Windows for {}",
        path.display()
    )
}

async fn spawn_thumbnail_job(path: PathBuf, size: u32) -> Result<GeneratedThumbnail, String> {
    tauri::async_runtime::spawn_blocking(move || {
        generate_thumbnail(&path, size).map_err(|error| {
            anyhow::anyhow!(
                "could not generate thumbnail for {}: {error}",
                path.display()
            )
        })
    })
    .await
    .map_err(|error| error.to_string())?
    .map_err(error_message)
}
