#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use anyhow::{Context, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use picturious_core::{
    FolderMetadata, FolderSummary, FolderView, FolderViewHeader, FolderViewTarget,
    GeneratedThumbnail, ImageMetadata, ImageSummary, LibraryManager, LibraryOverview, LibraryRoot,
    MetadataPersonSummary, MetadataSearchQuery, MetadataTag, RootDatabase,
    RotationDirection as CoreRotationDirection, ScanReport, ScanTarget, ThumbnailCache,
    ThumbnailResponse, convert_png_to_jpg as convert_png_to_jpg_file, generate_image_preview,
    generate_image_preview_jpeg, generate_thumbnail, rotate_image as rotate_image_file,
};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, hash_map::DefaultHasher};
use std::fmt::Write as _;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tauri::{
    AppHandle, Emitter, Manager, Monitor, PhysicalPosition, PhysicalSize, Position, Size, State,
    WebviewWindow, WindowEvent, ipc::Response,
};

const KLUTZGAMES_HOMEPAGE: &str = "https://www.klutzgames.com";
const DEFAULT_WINDOW_WIDTH: u32 = 1280;
const DEFAULT_WINDOW_HEIGHT: u32 = 820;
const MIN_RESTORED_WINDOW_WIDTH: u32 = 640;
const MIN_RESTORED_WINDOW_HEIGHT: u32 = 480;
const MONITOR_SIZED_WINDOW_TOLERANCE: u32 = 8;
const PREVIEW_CACHE_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const PREVIEW_CACHE_TARGET_BYTES: u64 = 1536 * 1024 * 1024;
const IMAGE_PREVIEW_CACHE_VERSION: u8 = 2;
const HDR_VIEWER_LOG_ENV: &str = "PICTURIOUS_HDR_VIEWER_LOG";
const HDR_VIEWER_DIAGNOSTICS_ENV: &str = "PICTURIOUS_HDR_VIEWER_DIAGNOSTICS";

struct AppState {
    library: Arc<Mutex<LibraryManager>>,
    thumbnails: Arc<Mutex<ThumbnailCache>>,
    active_scans: Arc<Mutex<HashSet<String>>>,
    active_movie_jobs: Arc<Mutex<HashMap<String, MovieJobControl>>>,
    settings: Arc<Mutex<UiSettings>>,
    settings_path: Arc<PathBuf>,
    preview_cache_dir: Arc<PathBuf>,
    hdr_viewer: Arc<Mutex<Option<HdrViewerProcess>>>,
    window_state_suppressed_until: Arc<Mutex<Option<Instant>>>,
    window_closing: Arc<AtomicBool>,
}

struct ActiveRootGuard {
    active_scans: Arc<Mutex<HashSet<String>>>,
    root_id: String,
}

impl Drop for ActiveRootGuard {
    fn drop(&mut self) {
        if let Ok(mut active_scans) = self.active_scans.lock() {
            active_scans.remove(&self.root_id);
        }
    }
}

#[derive(Clone)]
struct MovieJobControl {
    cancel_requested: Arc<AtomicBool>,
    child_id: Arc<Mutex<Option<u32>>>,
}

struct HdrViewerProcess {
    child: Child,
    stdin: ChildStdin,
    #[cfg(windows)]
    _job: Option<HdrViewerJob>,
}

#[cfg(windows)]
struct HdrViewerJob {
    handle: windows_sys::Win32::Foundation::HANDLE,
}

#[cfg(windows)]
unsafe impl Send for HdrViewerJob {}

#[cfg(windows)]
impl Drop for HdrViewerJob {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe {
                windows_sys::Win32::Foundation::CloseHandle(self.handle);
            }
        }
    }
}

impl Drop for HdrViewerProcess {
    fn drop(&mut self) {
        let _ = serde_json::to_writer(&mut self.stdin, &HdrViewerCommand::Close);
        let _ = writeln!(self.stdin);
        let _ = self.stdin.flush();

        for _ in 0..10 {
            if matches!(self.child.try_wait(), Ok(Some(_))) {
                return;
            }
            thread::sleep(Duration::from_millis(30));
        }

        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct TempPathCleanup {
    path: PathBuf,
}

impl TempPathCleanup {
    fn file(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for TempPathCleanup {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
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
    #[serde(default = "default_jpg_quality")]
    jpg_quality: u8,
    #[serde(default)]
    movie_create_enabled: bool,
    #[serde(default)]
    ffmpeg_path: String,
    #[serde(default)]
    movie_codec: MovieCodec,
    #[serde(default)]
    movie_quality: MovieQuality,
    #[serde(default)]
    movie_output_folder: String,
    #[serde(default)]
    movie_resolution: MovieResolution,
    #[serde(default = "default_movie_custom_resolution")]
    movie_custom_resolution: String,
    #[serde(default)]
    movie_mode: MovieMode,
    #[serde(default = "default_movie_fps")]
    movie_fps: u32,
    #[serde(default = "default_movie_slideshow_seconds")]
    movie_slideshow_seconds: f64,
    #[serde(default)]
    external_viewers: Vec<ExternalViewer>,
    #[serde(default)]
    window: Option<WindowSettings>,
    #[serde(default)]
    window_mode: WindowMode,
}

impl Default for UiSettings {
    fn default() -> Self {
        Self {
            thumb_scale: default_thumb_scale(),
            upscale_fullscreen_images: false,
            slideshow_speed_seconds: default_slideshow_speed_seconds(),
            slideshow_loop: false,
            slideshow_ignore_smaller_than: 0,
            jpg_quality: default_jpg_quality(),
            movie_create_enabled: false,
            ffmpeg_path: String::new(),
            movie_codec: MovieCodec::default(),
            movie_quality: MovieQuality::default(),
            movie_output_folder: String::new(),
            movie_resolution: MovieResolution::default(),
            movie_custom_resolution: default_movie_custom_resolution(),
            movie_mode: MovieMode::default(),
            movie_fps: default_movie_fps(),
            movie_slideshow_seconds: default_movie_slideshow_seconds(),
            external_viewers: Vec::new(),
            window: None,
            window_mode: WindowMode::Normal,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ExternalViewer {
    id: String,
    name: String,
    path: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
enum MovieCodec {
    #[default]
    H264,
    H265,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
enum MovieQuality {
    High,
    #[default]
    Balanced,
    Small,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
enum MovieResolution {
    #[serde(rename = "720p")]
    P720,
    #[default]
    #[serde(rename = "1080p")]
    P1080,
    #[serde(rename = "4k")]
    P4k,
    Custom,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
enum MovieMode {
    #[default]
    Movie,
    Slideshow,
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
    #[serde(default = "default_jpg_quality")]
    jpg_quality: u8,
    #[serde(default)]
    movie_create_enabled: bool,
    #[serde(default)]
    ffmpeg_path: String,
    #[serde(default)]
    movie_codec: MovieCodec,
    #[serde(default)]
    movie_quality: MovieQuality,
    #[serde(default)]
    movie_output_folder: String,
    #[serde(default)]
    movie_resolution: MovieResolution,
    #[serde(default = "default_movie_custom_resolution")]
    movie_custom_resolution: String,
    #[serde(default)]
    movie_mode: MovieMode,
    #[serde(default = "default_movie_fps")]
    movie_fps: u32,
    #[serde(default = "default_movie_slideshow_seconds")]
    movie_slideshow_seconds: f64,
    #[serde(default)]
    external_viewers: Vec<ExternalViewer>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WindowSettings {
    x: i32,
    y: i32,
    width: u32,
    height: u32,
    #[serde(default, skip_serializing)]
    maximized: bool,
    #[serde(default, skip_serializing)]
    fullscreen: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WindowMode {
    Normal,
    Maximized,
    Fullscreen,
}

impl Default for WindowMode {
    fn default() -> Self {
        Self::Normal
    }
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

#[derive(Clone, Serialize)]
struct PngConversionReport {
    converted: u32,
}

#[derive(Clone, Serialize)]
struct MovieCreationReport {
    output_path: String,
    image_count: u32,
}

#[derive(Clone, Serialize)]
struct MovieOutputPreview {
    output_path: String,
    exists: bool,
    image_count: u32,
}

#[derive(Clone, Serialize)]
struct ImagePreviewFileResponse {
    image_id: i64,
    path: String,
    from_cache: bool,
}

#[derive(Clone, Serialize)]
struct HdrViewerBounds {
    x: i32,
    y: i32,
    width: i32,
    height: i32,
}

#[derive(Serialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
enum HdrViewerCommand {
    Show {
        path: String,
        generation: u64,
        bounds: Option<HdrViewerBounds>,
        cursor_hidden: bool,
    },
    SetCursorHidden {
        hidden: bool,
    },
    Hide,
    Close,
}

#[derive(Clone, Serialize)]
struct MovieCreationStarted {
    job_id: String,
    output_path: String,
    image_count: u32,
}

#[derive(Clone, Serialize)]
struct MovieCreationOutput {
    job_id: String,
    stream: String,
    text: String,
}

#[derive(Clone, Serialize)]
struct MovieCreationFinished {
    job_id: String,
    output_path: String,
    image_count: u32,
    success: bool,
    canceled: bool,
    message: String,
}

#[derive(Debug, Clone)]
struct MovieJobSettings {
    ffmpeg_path: PathBuf,
    codec: MovieCodec,
    quality: MovieQuality,
    output_folder: Option<PathBuf>,
    width: u32,
    height: u32,
    mode: MovieMode,
    fps: u32,
    slideshow_seconds: f64,
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
    settings.jpg_quality = preferences.jpg_quality;
    settings.movie_create_enabled = preferences.movie_create_enabled;
    settings.ffmpeg_path = preferences.ffmpeg_path;
    settings.movie_codec = preferences.movie_codec;
    settings.movie_quality = preferences.movie_quality;
    settings.movie_output_folder = preferences.movie_output_folder;
    settings.movie_resolution = preferences.movie_resolution;
    settings.movie_custom_resolution = preferences.movie_custom_resolution;
    settings.movie_mode = preferences.movie_mode;
    settings.movie_fps = preferences.movie_fps;
    settings.movie_slideshow_seconds = preferences.movie_slideshow_seconds;
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
fn pick_ffmpeg_executable() -> Option<String> {
    let mut dialog = rfd::FileDialog::new()
        .set_title("Choose ffmpeg.exe")
        .add_filter("Programs", &["exe"]);
    if let Some(program_files) = std::env::var_os("ProgramFiles") {
        dialog = dialog.set_directory(PathBuf::from(program_files));
    }

    dialog.pick_file().map(|path| clean_path_string(&path))
}

#[tauri::command]
fn pick_movie_output_folder() -> Option<String> {
    rfd::FileDialog::new()
        .set_title("Choose Movie Output Folder")
        .pick_folder()
        .map(|path| clean_path_string(&path))
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

    if is_supported_3d_asset_path(&path) {
        let library = state.library.clone();
        let root_id_for_thumbnail = root_id.clone();
        let stored_thumbnail = tauri::async_runtime::spawn_blocking(move || {
            library
                .lock()
                .map_err(|_| "library state is locked".to_owned())?
                .asset_thumbnail(&root_id_for_thumbnail, image_id)
                .map_err(error_message)
        })
        .await
        .map_err(|error| error.to_string())??;
        let placeholder: fn() -> String = if is_supported_splat_path(&path) {
            splat_placeholder_data_url
        } else {
            model_placeholder_data_url
        };

        return Ok(ThumbnailResponse {
            image_id,
            data_url: stored_thumbnail
                .map(|thumbnail| image_data_url(&thumbnail.mime_type, &thumbnail.data))
                .unwrap_or_else(placeholder),
            from_cache: false,
        });
    }

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
async fn image_preview(
    root_id: String,
    image_id: i64,
    size: u32,
    state: State<'_, AppState>,
) -> Result<ThumbnailResponse, String> {
    if root_is_scanning(&state.active_scans, &root_id)? {
        return Err("image preview generation is paused while scanning".to_owned());
    }

    let library = state.library.clone();
    let root_id_for_path = root_id.clone();
    let (path, _) = tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .image_path(&root_id_for_path, image_id)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())??;

    if is_supported_3d_asset_path(&path) {
        return Err("image preview is only available for regular images".to_owned());
    }

    let generated = spawn_image_preview_job(path, size).await?;
    Ok(generated.response(image_id, false))
}

#[tauri::command]
async fn image_preview_file(
    root_id: String,
    image_id: i64,
    size: u32,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<ImagePreviewFileResponse, String> {
    if root_is_scanning(&state.active_scans, &root_id)? {
        return Err("image preview generation is paused while scanning".to_owned());
    }

    let library = state.library.clone();
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

    if is_supported_3d_asset_path(&path) {
        return Err("image preview is only available for regular images".to_owned());
    }

    let size = size.clamp(64, 8192);
    let cache_path = image_preview_cache_path(
        state.preview_cache_dir.as_ref(),
        &root_id,
        image_id,
        modified_unix_ms,
        size,
        &path,
    );
    let from_cache = cache_path.is_file();
    if !from_cache {
        let source_path = path.clone();
        let cache_path_for_job = cache_path.clone();
        let cache_dir = state.preview_cache_dir.clone();
        tauri::async_runtime::spawn_blocking(move || -> anyhow::Result<()> {
            fs::create_dir_all(cache_dir.as_ref())?;
            if !cache_path_for_job.is_file() {
                let bytes = generate_image_preview_jpeg(&source_path, size).map_err(|error| {
                    anyhow::anyhow!(
                        "could not generate preview for {}: {error}",
                        source_path.display()
                    )
                })?;
                fs::write(&cache_path_for_job, bytes)?;
                prune_preview_cache(cache_dir.as_ref());
            }
            Ok(())
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(error_message)?;
    }

    app.asset_protocol_scope()
        .allow_file(&cache_path)
        .map_err(|error| error.to_string())?;

    Ok(ImagePreviewFileResponse {
        image_id,
        path: clean_path_string(&cache_path),
        from_cache,
    })
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
async fn splat_file_bytes(
    root_id: String,
    image_id: i64,
    state: State<'_, AppState>,
) -> Result<Response, String> {
    let (path, _) = image_path_for(&state.library, &root_id, image_id)?;
    if !is_supported_splat_path(&path) {
        return Err("file is not a supported 3DGS file".to_owned());
    }

    let read_path = path.clone();
    let bytes = tauri::async_runtime::spawn_blocking(move || {
        fs::read(&read_path)
            .map_err(|error| format!("could not read {}: {error}", read_path.display()))
    })
    .await
    .map_err(|error| error.to_string())??;

    Ok(Response::new(bytes))
}

#[tauri::command]
async fn asset_file_bytes(
    root_id: String,
    image_id: i64,
    state: State<'_, AppState>,
) -> Result<Response, String> {
    let (path, _) = image_path_for(&state.library, &root_id, image_id)?;
    if !is_supported_3d_asset_path(&path) {
        return Err("file is not a supported 3D asset".to_owned());
    }

    let read_path = path.clone();
    let bytes = tauri::async_runtime::spawn_blocking(move || {
        fs::read(&read_path)
            .map_err(|error| format!("could not read {}: {error}", read_path.display()))
    })
    .await
    .map_err(|error| error.to_string())??;

    Ok(Response::new(bytes))
}

#[tauri::command]
fn splat_camera_state(
    root_id: String,
    image_id: i64,
    state: State<'_, AppState>,
) -> Result<Option<serde_json::Value>, String> {
    let (path, _) = image_path_for(&state.library, &root_id, image_id)?;
    if !is_supported_splat_path(&path) {
        return Err("camera restore is only available for 3DGS files".to_owned());
    }

    let thumbnail = state
        .library
        .lock()
        .map_err(|_| "library state is locked".to_owned())?
        .splat_thumbnail(&root_id, image_id)
        .map_err(error_message)?;

    thumbnail
        .and_then(|thumbnail| thumbnail.camera_json)
        .map(|camera_json| {
            serde_json::from_str(&camera_json)
                .map_err(|error| format!("could not parse saved 3DGS camera state: {error}"))
        })
        .transpose()
}

#[tauri::command]
fn asset_camera_state(
    root_id: String,
    image_id: i64,
    state: State<'_, AppState>,
) -> Result<Option<serde_json::Value>, String> {
    let (path, _) = image_path_for(&state.library, &root_id, image_id)?;
    if !is_supported_3d_asset_path(&path) {
        return Err("camera restore is only available for 3D assets".to_owned());
    }

    let thumbnail = state
        .library
        .lock()
        .map_err(|_| "library state is locked".to_owned())?
        .asset_thumbnail(&root_id, image_id)
        .map_err(error_message)?;

    thumbnail
        .and_then(|thumbnail| thumbnail.camera_json)
        .map(|camera_json| {
            serde_json::from_str(&camera_json)
                .map_err(|error| format!("could not parse saved 3D camera state: {error}"))
        })
        .transpose()
}

#[tauri::command]
fn save_splat_thumbnail(
    root_id: String,
    image_id: i64,
    data_url: String,
    camera_state: Option<serde_json::Value>,
    state: State<'_, AppState>,
) -> Result<String, String> {
    let (path, _) = image_path_for(&state.library, &root_id, image_id)?;
    if !is_supported_splat_path(&path) {
        return Err("thumbnail capture is only available for 3DGS files".to_owned());
    }

    let (mime_type, bytes) = decode_image_data_url(&data_url)?;
    let camera_json = camera_state
        .map(|state| {
            serde_json::to_string(&state)
                .map_err(|error| format!("could not serialize 3DGS camera state: {error}"))
        })
        .transpose()?;
    state
        .library
        .lock()
        .map_err(|_| "library state is locked".to_owned())?
        .save_splat_thumbnail(
            &root_id,
            image_id,
            &mime_type,
            &bytes,
            camera_json.as_deref(),
        )
        .map_err(error_message)?;

    Ok("database".to_owned())
}

#[tauri::command]
fn save_asset_thumbnail(
    root_id: String,
    image_id: i64,
    data_url: String,
    camera_state: Option<serde_json::Value>,
    state: State<'_, AppState>,
) -> Result<String, String> {
    let (path, _) = image_path_for(&state.library, &root_id, image_id)?;
    if !is_supported_3d_asset_path(&path) {
        return Err("thumbnail capture is only available for 3D assets".to_owned());
    }

    let (mime_type, bytes) = decode_image_data_url(&data_url)?;
    let camera_json = camera_state
        .map(|state| {
            serde_json::to_string(&state)
                .map_err(|error| format!("could not serialize 3D camera state: {error}"))
        })
        .transpose()?;
    state
        .library
        .lock()
        .map_err(|_| "library state is locked".to_owned())?
        .save_asset_thumbnail(
            &root_id,
            image_id,
            &mime_type,
            &bytes,
            camera_json.as_deref(),
        )
        .map_err(error_message)?;

    Ok("database".to_owned())
}

#[tauri::command]
fn set_viewer_fullscreen(
    fullscreen: bool,
    window: WebviewWindow,
    state: State<'_, AppState>,
) -> Result<(), String> {
    suppress_window_state_tracking(
        &state.window_state_suppressed_until,
        Duration::from_millis(2500),
    );
    if fullscreen {
        save_windowed_state_before_fullscreen(
            &window,
            &state.settings,
            state.settings_path.as_ref().as_path(),
        )
        .map_err(error_message)?;
        window.set_focus().map_err(|error| error.to_string())?;
        window
            .set_fullscreen(true)
            .map_err(|error| error.to_string())?;
        set_saved_window_mode(
            &state.settings,
            state.settings_path.as_ref().as_path(),
            WindowMode::Fullscreen,
        )
        .map_err(error_message)?;
        window.set_focus().map_err(|error| error.to_string())?;
    } else {
        window
            .set_fullscreen(false)
            .map_err(|error| error.to_string())?;
        restore_saved_fullscreen_window(
            &window,
            &state.settings,
            state.settings_path.as_ref().as_path(),
        )
        .map_err(error_message)?;
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
async fn convert_image_png_to_jpg(
    root_id: String,
    image_id: i64,
    folder_relative_path: String,
    state: State<'_, AppState>,
) -> Result<PngConversionReport, String> {
    let _active_root = begin_active_root_job(
        &state.active_scans,
        &root_id,
        "PNG conversion is paused while scanning",
    )?;

    let quality = state
        .settings
        .lock()
        .map_err(|_| "app settings are locked".to_owned())?
        .jpg_quality;
    let library = state.library.clone();
    let (path, _) = image_path_for(&library, &root_id, image_id)?;

    tauri::async_runtime::spawn_blocking(move || convert_png_to_jpg_file(&path, quality))
        .await
        .map_err(|error| error.to_string())?
        .map_err(error_message)?;

    scan_folder_blocking(library, root_id, folder_relative_path).await?;
    Ok(PngConversionReport { converted: 1 })
}

#[tauri::command]
async fn convert_folder_pngs_to_jpg(
    root_id: String,
    relative_path: String,
    state: State<'_, AppState>,
) -> Result<PngConversionReport, String> {
    let _active_root = begin_active_root_job(
        &state.active_scans,
        &root_id,
        "PNG conversion is paused while scanning",
    )?;

    let quality = state
        .settings
        .lock()
        .map_err(|_| "app settings are locked".to_owned())?
        .jpg_quality;
    let library = state.library.clone();
    let folder_path = folder_path_for(&library, &root_id, &relative_path)?;
    let report = tauri::async_runtime::spawn_blocking(move || {
        convert_png_folder_recursive(&folder_path, quality)
    })
    .await
    .map_err(|error| error.to_string())?
    .map_err(error_message)?;

    if report.converted > 0 {
        scan_folder_blocking(library, root_id, relative_path).await?;
    }

    Ok(report)
}

#[tauri::command]
async fn movie_output_preview(
    root_id: String,
    relative_path: String,
    state: State<'_, AppState>,
) -> Result<MovieOutputPreview, String> {
    let settings = state
        .settings
        .lock()
        .map_err(|_| "app settings are locked".to_owned())?
        .clone();
    if !settings.movie_create_enabled {
        return Err("movie creation is disabled".to_owned());
    }

    let job = movie_job_settings(&settings)?;
    let folder_path = folder_path_for(&state.library, &root_id, &relative_path)?;
    tauri::async_runtime::spawn_blocking(move || preview_movie_output(&folder_path, &job))
        .await
        .map_err(|error| error.to_string())?
        .map_err(error_message)
}

#[tauri::command]
async fn start_movie_creation(
    root_id: String,
    relative_path: String,
    overwrite: bool,
    job_id: String,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<MovieCreationStarted, String> {
    let job_id = sanitize_movie_job_id(&job_id)?;
    let settings = state
        .settings
        .lock()
        .map_err(|_| "app settings are locked".to_owned())?
        .clone();
    if !settings.movie_create_enabled {
        return Err("movie creation is disabled".to_owned());
    }

    let job = movie_job_settings(&settings)?;
    let folder_path = folder_path_for(&state.library, &root_id, &relative_path)?;
    let preview = preview_movie_output(&folder_path, &job).map_err(error_message)?;
    if preview.exists && !overwrite {
        return Err(format!(
            "movie output already exists: {}",
            preview.output_path
        ));
    }

    let control = MovieJobControl {
        cancel_requested: Arc::new(AtomicBool::new(false)),
        child_id: Arc::new(Mutex::new(None)),
    };
    {
        let mut active_jobs = state
            .active_movie_jobs
            .lock()
            .map_err(|_| "movie job state is locked".to_owned())?;
        if active_jobs
            .insert(job_id.clone(), control.clone())
            .is_some()
        {
            return Err("movie job id is already active".to_owned());
        }
    }

    let active_movie_jobs = state.active_movie_jobs.clone();
    let started = MovieCreationStarted {
        job_id: job_id.clone(),
        output_path: preview.output_path.clone(),
        image_count: preview.image_count,
    };
    let _ = tauri::async_runtime::spawn_blocking(move || {
        run_movie_creation_job(
            job_id,
            folder_path,
            job,
            overwrite,
            preview.output_path,
            preview.image_count,
            control,
            app,
            active_movie_jobs,
        );
    });

    Ok(started)
}

#[tauri::command]
fn cancel_movie_creation(job_id: String, state: State<'_, AppState>) -> Result<bool, String> {
    let control = state
        .active_movie_jobs
        .lock()
        .map_err(|_| "movie job state is locked".to_owned())?
        .get(&job_id)
        .cloned();
    let Some(control) = control else {
        return Ok(false);
    };

    control.cancel_requested.store(true, AtomicOrdering::SeqCst);
    if let Some(child_id) = *control
        .child_id
        .lock()
        .map_err(|_| "movie process state is locked".to_owned())?
    {
        terminate_process_tree(child_id).map_err(error_message)?;
    }

    Ok(true)
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
fn show_folder_in_explorer(
    root_id: String,
    relative_path: String,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let path = folder_path_for(&state.library, &root_id, &relative_path)?;
    show_in_explorer(&path).map_err(error_message)
}

#[tauri::command]
fn open_homepage() -> Result<(), String> {
    open_url_in_default_browser(KLUTZGAMES_HOMEPAGE).map_err(error_message)
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
fn open_image_default(
    root_id: String,
    image_id: i64,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let (path, _) = image_path_for(&state.library, &root_id, image_id)?;
    open_path_in_default_app(&path).map_err(error_message)
}

#[tauri::command]
fn open_image_hdr_viewer(
    root_id: String,
    image_id: i64,
    window: WebviewWindow,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let (path, _) = image_path_for(&state.library, &root_id, image_id)?;
    let bounds = hdr_viewer_bounds_for_window(&window);
    let command = HdrViewerCommand::Show {
        path: clean_path_string(&path),
        generation: 0,
        bounds,
        cursor_hidden: false,
    };
    send_hdr_viewer_command(&app, &state, &command).map_err(error_message)
}

#[tauri::command]
fn hdr_viewer_show(
    root_id: String,
    image_id: i64,
    generation: u64,
    cursor_hidden: bool,
    window: WebviewWindow,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let (path, _) = image_path_for(&state.library, &root_id, image_id)?;
    let bounds = hdr_viewer_bounds_for_window(&window);
    let command = HdrViewerCommand::Show {
        path: clean_path_string(&path),
        generation,
        bounds,
        cursor_hidden,
    };
    trace_hdr_viewer(
        &state,
        "rust_show_command",
        Some(generation),
        &format!("root_id={root_id} image_id={image_id}"),
    );
    send_hdr_viewer_command(&app, &state, &command).map_err(error_message)
}

#[tauri::command]
fn hdr_viewer_hide(state: State<'_, AppState>) -> Result<(), String> {
    trace_hdr_viewer(&state, "rust_hide_command", None, "");
    send_hdr_viewer_command_if_running(&state, &HdrViewerCommand::Hide).map_err(error_message)
}

#[tauri::command]
fn hdr_viewer_set_cursor_hidden(hidden: bool, state: State<'_, AppState>) -> Result<(), String> {
    trace_hdr_viewer(
        &state,
        "rust_cursor_hidden_command",
        None,
        &format!("hidden={hidden}"),
    );
    send_hdr_viewer_command_if_running(&state, &HdrViewerCommand::SetCursorHidden { hidden })
        .map_err(error_message)
}

#[tauri::command]
fn hdr_viewer_trace(
    label: String,
    generation: Option<u64>,
    detail: Option<String>,
    state: State<'_, AppState>,
) -> Result<(), String> {
    trace_hdr_viewer(&state, &label, generation, detail.as_deref().unwrap_or(""));
    Ok(())
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
async fn move_folder_to_recycle_bin(
    root_id: String,
    relative_path: String,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let library = state.library.clone();
    let path = folder_path_for(&library, &root_id, &relative_path)?;
    tauri::async_runtime::spawn_blocking(move || recycle_file(&path))
        .await
        .map_err(|error| error.to_string())?
        .map_err(error_message)?;

    library
        .lock()
        .map_err(|_| "library state is locked".to_owned())?
        .delete_folder(&root_id, &relative_path)
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

#[tauri::command]
fn set_folder_thumbnail_by_path(
    root_id: String,
    relative_path: String,
    image_id: i64,
    state: State<'_, AppState>,
) -> Result<(), String> {
    state
        .library
        .lock()
        .map_err(|_| "library state is locked".to_owned())?
        .set_folder_thumbnail_by_path(&root_id, &relative_path, image_id)
        .map_err(error_message)
}

#[tauri::command]
async fn image_metadata(
    root_id: String,
    image_id: i64,
    state: State<'_, AppState>,
) -> Result<ImageMetadata, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .image_metadata(&root_id, image_id)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn image_people(
    root_id: String,
    state: State<'_, AppState>,
) -> Result<Vec<MetadataTag>, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .people(&root_id)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn metadata_people(state: State<'_, AppState>) -> Result<Vec<MetadataTag>, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .all_people()
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn metadata_tags(state: State<'_, AppState>) -> Result<Vec<MetadataTag>, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .all_keywords()
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn rename_metadata_person(
    old_name: String,
    new_name: String,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .rename_person_everywhere(&old_name, &new_name)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn delete_metadata_person(name: String, state: State<'_, AppState>) -> Result<(), String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .delete_person_everywhere(&name)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn rename_metadata_tag(
    old_name: String,
    new_name: String,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .rename_keyword_everywhere(&old_name, &new_name)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn delete_metadata_tag(name: String, state: State<'_, AppState>) -> Result<(), String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .delete_keyword_everywhere(&name)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn metadata_person_thumbnails(
    state: State<'_, AppState>,
) -> Result<Vec<MetadataPersonSummary>, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .all_people_with_thumbnails()
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn metadata_filtered_person_thumbnails(
    query: MetadataSearchQuery,
    state: State<'_, AppState>,
) -> Result<Vec<MetadataPersonSummary>, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .filtered_people_with_thumbnails(&query)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn metadata_search(
    query: MetadataSearchQuery,
    state: State<'_, AppState>,
) -> Result<Vec<FolderSummary>, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .search_metadata(&query)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn folder_metadata(
    root_id: String,
    folder_id: i64,
    state: State<'_, AppState>,
) -> Result<FolderMetadata, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .folder_metadata(&root_id, folder_id)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn add_folder_tag(
    root_id: String,
    folder_id: i64,
    name: String,
    state: State<'_, AppState>,
) -> Result<FolderMetadata, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .add_folder_keyword(&root_id, folder_id, &name)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn remove_folder_tag(
    root_id: String,
    folder_id: i64,
    tag_id: i64,
    state: State<'_, AppState>,
) -> Result<FolderMetadata, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .remove_folder_keyword(&root_id, folder_id, tag_id)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn add_folder_person(
    root_id: String,
    folder_id: i64,
    name: String,
    state: State<'_, AppState>,
) -> Result<FolderMetadata, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .add_folder_person(&root_id, folder_id, &name)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn remove_folder_person(
    root_id: String,
    folder_id: i64,
    person_id: i64,
    state: State<'_, AppState>,
) -> Result<FolderMetadata, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .remove_folder_person(&root_id, folder_id, person_id)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn set_folder_rating(
    root_id: String,
    folder_id: i64,
    rating: Option<u8>,
    state: State<'_, AppState>,
) -> Result<FolderMetadata, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .set_folder_rating(&root_id, folder_id, rating)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn add_image_person(
    root_id: String,
    image_id: i64,
    name: String,
    state: State<'_, AppState>,
) -> Result<ImageMetadata, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .add_image_person(&root_id, image_id, &name)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn remove_image_person(
    root_id: String,
    image_id: i64,
    person_id: i64,
    state: State<'_, AppState>,
) -> Result<ImageMetadata, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .remove_image_person(&root_id, image_id, person_id)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn set_image_rating(
    root_id: String,
    image_id: i64,
    rating: Option<u8>,
    state: State<'_, AppState>,
) -> Result<ImageMetadata, String> {
    let library = state.library.clone();
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| "library state is locked".to_owned())?
            .set_image_rating(&root_id, image_id, rating)
            .map_err(error_message)
    })
    .await
    .map_err(|error| error.to_string())?
}

fn main() {
    tauri::Builder::default()
        .setup(|app| {
            let config_dir = app.path().app_config_dir()?;
            let settings_dir = app.path().app_local_data_dir()?;
            let settings_path = Arc::new(settings_dir.join("settings.json"));
            let preview_cache_dir = Arc::new(settings_dir.join("preview-cache"));
            let settings = Arc::new(Mutex::new(read_ui_settings(settings_path.as_ref())));
            app.manage(AppState {
                library: Arc::new(Mutex::new(LibraryManager::new(&config_dir)?)),
                thumbnails: Arc::new(Mutex::new(ThumbnailCache::default())),
                active_scans: Arc::new(Mutex::new(HashSet::new())),
                active_movie_jobs: Arc::new(Mutex::new(HashMap::new())),
                settings: settings.clone(),
                settings_path: settings_path.clone(),
                preview_cache_dir,
                hdr_viewer: Arc::new(Mutex::new(None)),
                window_state_suppressed_until: Arc::new(Mutex::new(None)),
                window_closing: Arc::new(AtomicBool::new(false)),
            });
            if let Some(window) = app.get_webview_window("main") {
                if let Ok(settings_snapshot) = settings.lock().map(|settings| settings.clone()) {
                    if settings_snapshot.window_mode != WindowMode::Normal {
                        let state = app.state::<AppState>();
                        suppress_window_state_tracking(
                            &state.window_state_suppressed_until,
                            Duration::from_secs(3),
                        );
                    }
                    let _ = restore_window_state(&window, &settings_snapshot);
                }
                let state = app.state::<AppState>();
                track_window_state(
                    &window,
                    settings,
                    settings_path,
                    state.window_state_suppressed_until.clone(),
                    state.window_closing.clone(),
                    state.hdr_viewer.clone(),
                );
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
            pick_ffmpeg_executable,
            pick_movie_output_folder,
            add_root,
            remove_root,
            start_scan,
            folder_view,
            recursive_folder_images,
            stream_folder_view,
            validate_folder_view,
            thumbnail,
            image_preview,
            image_preview_file,
            image_file_path,
            splat_file_bytes,
            asset_file_bytes,
            splat_camera_state,
            asset_camera_state,
            save_splat_thumbnail,
            save_asset_thumbnail,
            set_viewer_fullscreen,
            rotate_image,
            convert_image_png_to_jpg,
            convert_folder_pngs_to_jpg,
            movie_output_preview,
            start_movie_creation,
            cancel_movie_creation,
            show_image_in_explorer,
            show_folder_in_explorer,
            open_homepage,
            open_image_with,
            open_image_default,
            open_image_hdr_viewer,
            hdr_viewer_show,
            hdr_viewer_hide,
            hdr_viewer_set_cursor_hidden,
            hdr_viewer_trace,
            move_image_to_recycle_bin,
            move_folder_to_recycle_bin,
            set_folder_thumbnail,
            set_folder_thumbnail_by_path,
            image_metadata,
            image_people,
            metadata_people,
            metadata_tags,
            rename_metadata_person,
            delete_metadata_person,
            rename_metadata_tag,
            delete_metadata_tag,
            metadata_person_thumbnails,
            metadata_filtered_person_thumbnails,
            metadata_search,
            folder_metadata,
            add_folder_tag,
            remove_folder_tag,
            add_folder_person,
            remove_folder_person,
            set_folder_rating,
            add_image_person,
            remove_image_person,
            set_image_rating
        ])
        .run(tauri::generate_context!())
        .expect("failed to run Picturious");
}

fn external_viewer_for_path(path: &Path) -> ExternalViewer {
    let clean_path = clean_path_string(path);
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

fn clean_path_string(path: &Path) -> String {
    path.to_string_lossy()
        .trim_start_matches(r"\\?\")
        .to_owned()
}

fn hdr_viewer_executable_path() -> anyhow::Result<PathBuf> {
    let current_exe = std::env::current_exe().context("could not locate Picturious executable")?;
    let exe_dir = current_exe
        .parent()
        .context("Picturious executable has no parent directory")?;
    let viewer_name = if cfg!(windows) {
        "picturious-hdr-viewer.exe"
    } else {
        "picturious-hdr-viewer"
    };
    let candidate = exe_dir.join(viewer_name);
    if candidate.is_file() {
        return Ok(candidate);
    }

    anyhow::bail!(
        "DirectX HDR viewer helper was not found at {}; build it with `cargo build -p picturious --bin picturious-hdr-viewer`",
        candidate.display()
    )
}

fn hdr_viewer_bounds_for_window(window: &WebviewWindow) -> Option<HdrViewerBounds> {
    let monitor = window.current_monitor().ok().flatten()?;
    let position = monitor.position();
    let size = monitor.size();
    Some(HdrViewerBounds {
        x: position.x,
        y: position.y,
        width: i32::try_from(size.width).ok()?,
        height: i32::try_from(size.height).ok()?,
    })
}

fn send_hdr_viewer_command(
    app: &AppHandle,
    state: &AppState,
    command: &HdrViewerCommand,
) -> anyhow::Result<()> {
    ensure_hdr_viewer_process(app, state)?;
    let mut guard = state
        .hdr_viewer
        .lock()
        .map_err(|_| anyhow::anyhow!("HDR viewer process state is locked"))?;
    let process = guard
        .as_mut()
        .context("HDR viewer process was not started")?;
    write_hdr_viewer_command(process, command)?;
    Ok(())
}

fn send_hdr_viewer_command_if_running(
    state: &AppState,
    command: &HdrViewerCommand,
) -> anyhow::Result<()> {
    let mut guard = state
        .hdr_viewer
        .lock()
        .map_err(|_| anyhow::anyhow!("HDR viewer process state is locked"))?;
    let exited = match guard.as_mut() {
        Some(process) => process.child.try_wait()?.is_some(),
        None => return Ok(()),
    };
    if exited {
        *guard = None;
        return Ok(());
    }

    let process = guard
        .as_mut()
        .context("HDR viewer process was not started")?;
    write_hdr_viewer_command(process, command)?;
    Ok(())
}

fn write_hdr_viewer_command(
    process: &mut HdrViewerProcess,
    command: &HdrViewerCommand,
) -> anyhow::Result<()> {
    serde_json::to_writer(&mut process.stdin, command)?;
    writeln!(process.stdin)?;
    process.stdin.flush()?;
    Ok(())
}

fn ensure_hdr_viewer_process(app: &AppHandle, state: &AppState) -> anyhow::Result<()> {
    let mut guard = state
        .hdr_viewer
        .lock()
        .map_err(|_| anyhow::anyhow!("HDR viewer process state is locked"))?;

    if let Some(process) = guard.as_mut() {
        if process.child.try_wait()?.is_none() {
            return Ok(());
        }
    }

    *guard = None;
    let viewer_path = hdr_viewer_executable_path()?;
    let mut command = Command::new(&viewer_path);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
    }
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let log_path = hdr_viewer_log_path(state);
    if let Some(log_path) = &log_path {
        command.env(HDR_VIEWER_LOG_ENV, log_path);
        append_hdr_viewer_log(
            log_path,
            &format!("[{}] rust_trace label=rust_spawn_helper\n", unix_time_ms()),
        );
    }

    let mut child = command.spawn().with_context(|| {
        format!(
            "could not start DirectX HDR viewer at {}",
            viewer_path.display()
        )
    })?;
    let stdin = child
        .stdin
        .take()
        .context("DirectX HDR viewer did not provide stdin")?;
    if let Some(stdout) = child.stdout.take() {
        spawn_hdr_viewer_event_reader(app.clone(), stdout, log_path);
    }

    #[cfg(windows)]
    let job = match create_hdr_viewer_job(&child) {
        Ok(job) => Some(job),
        Err(error) => {
            eprintln!("could not attach DirectX HDR viewer to cleanup job: {error}");
            None
        }
    };

    *guard = Some(HdrViewerProcess {
        child,
        stdin,
        #[cfg(windows)]
        _job: job,
    });
    Ok(())
}

#[cfg(windows)]
fn create_hdr_viewer_job(child: &Child) -> anyhow::Result<HdrViewerJob> {
    use std::mem::size_of;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
        SetInformationJobObject,
    };

    let job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
    if job.is_null() {
        return Err(io::Error::last_os_error()).context("could not create HDR viewer job object");
    }

    let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    let info_set = unsafe {
        SetInformationJobObject(
            job,
            JobObjectExtendedLimitInformation,
            &limits as *const _ as *const _,
            size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        )
    };
    if info_set == 0 {
        let error = io::Error::last_os_error();
        unsafe {
            CloseHandle(job);
        }
        return Err(error).context("could not configure HDR viewer cleanup job");
    }

    let assigned = unsafe { AssignProcessToJobObject(job, child.as_raw_handle() as HANDLE) };
    if assigned == 0 {
        let error = io::Error::last_os_error();
        unsafe {
            CloseHandle(job);
        }
        return Err(error).context("could not attach HDR viewer process to cleanup job");
    }

    Ok(HdrViewerJob { handle: job })
}

fn shutdown_hdr_viewer_process(hdr_viewer: &Arc<Mutex<Option<HdrViewerProcess>>>) {
    let process = match hdr_viewer.lock() {
        Ok(mut guard) => guard.take(),
        Err(_) => None,
    };
    drop(process);
}

fn spawn_hdr_viewer_event_reader(
    app: AppHandle,
    stdout: impl Read + Send + 'static,
    log_path: Option<PathBuf>,
) {
    thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            let Ok(line) = line else {
                break;
            };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            match serde_json::from_str::<serde_json::Value>(trimmed) {
                Ok(event) => {
                    if let Some(log_path) = &log_path {
                        append_hdr_viewer_log(
                            log_path,
                            &format!(
                                "[{}] rust_trace label=helper_event event={}\n",
                                unix_time_ms(),
                                event
                            ),
                        );
                    }
                    let _ = app.emit("hdr-viewer-event", event);
                }
                Err(error) => {
                    eprintln!("could not parse DirectX HDR viewer event: {error}");
                }
            }
        }
    });
}

fn trace_hdr_viewer(state: &AppState, label: &str, generation: Option<u64>, detail: &str) {
    let Some(log_path) = hdr_viewer_log_path(state) else {
        return;
    };
    let generation = generation
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_owned());
    append_hdr_viewer_log(
        &log_path,
        &format!(
            "[{}] rust_trace label={} generation={} {}\n",
            unix_time_ms(),
            label,
            generation,
            detail
        ),
    );
}

fn hdr_viewer_log_path(state: &AppState) -> Option<PathBuf> {
    let log_env = std::env::var_os(HDR_VIEWER_LOG_ENV);
    let diagnostics_env = std::env::var_os(HDR_VIEWER_DIAGNOSTICS_ENV);
    hdr_viewer_log_path_for_settings(
        &state.settings_path,
        log_env.as_deref(),
        diagnostics_env.as_deref(),
    )
}

fn hdr_viewer_log_path_for_settings(
    settings_path: &Path,
    log_env: Option<&std::ffi::OsStr>,
    diagnostics_env: Option<&std::ffi::OsStr>,
) -> Option<PathBuf> {
    if let Some(value) = log_env {
        let normalized = value.to_string_lossy().trim().to_ascii_lowercase();
        if normalized.is_empty() || env_value_is_disabled(&normalized) {
            return None;
        }
        if env_value_is_enabled(&normalized) {
            return default_hdr_viewer_log_path(settings_path);
        }
        return Some(PathBuf::from(value));
    }

    if diagnostics_env
        .map(|value| env_value_is_enabled(value.to_string_lossy().trim()))
        .unwrap_or(false)
    {
        default_hdr_viewer_log_path(settings_path)
    } else {
        None
    }
}

fn default_hdr_viewer_log_path(settings_path: &Path) -> Option<PathBuf> {
    settings_path
        .parent()
        .map(|settings_dir| settings_dir.join("hdr-viewer.log"))
}

fn env_value_is_enabled(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn env_value_is_disabled(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "0" | "false" | "no" | "off"
    )
}

fn append_hdr_viewer_log(log_path: &Path, message: &str) {
    if let Some(parent) = log_path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(mut file) = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
    {
        let _ = file.write_all(message.as_bytes());
    }
}

fn unix_time_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn image_preview_cache_path(
    cache_dir: &Path,
    root_id: &str,
    image_id: i64,
    modified_unix_ms: i64,
    size: u32,
    source_path: &Path,
) -> PathBuf {
    let mut hasher = DefaultHasher::new();
    root_id.hash(&mut hasher);
    image_id.hash(&mut hasher);
    modified_unix_ms.hash(&mut hasher);
    size.hash(&mut hasher);
    IMAGE_PREVIEW_CACHE_VERSION.hash(&mut hasher);
    source_path.to_string_lossy().hash(&mut hasher);
    cache_dir.join(format!("{:016x}-{size}.jpg", hasher.finish()))
}

struct PreviewCacheFile {
    path: PathBuf,
    len: u64,
    modified: SystemTime,
}

fn prune_preview_cache(cache_dir: &Path) {
    let Ok(entries) = fs::read_dir(cache_dir) else {
        return;
    };

    let mut files = Vec::new();
    let mut total_bytes = 0_u64;
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if !metadata.is_file() {
            continue;
        }

        let len = metadata.len();
        total_bytes = total_bytes.saturating_add(len);
        files.push(PreviewCacheFile {
            path,
            len,
            modified: metadata.modified().unwrap_or(UNIX_EPOCH),
        });
    }

    if total_bytes <= PREVIEW_CACHE_MAX_BYTES {
        return;
    }

    files.sort_by_key(|file| file.modified);
    for file in files {
        if total_bytes <= PREVIEW_CACHE_TARGET_BYTES {
            break;
        }
        if fs::remove_file(&file.path).is_ok() {
            total_bytes = total_bytes.saturating_sub(file.len);
        }
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

async fn scan_folder_blocking(
    library: Arc<Mutex<LibraryManager>>,
    root_id: String,
    relative_path: String,
) -> Result<(), String> {
    tauri::async_runtime::spawn_blocking(move || {
        library
            .lock()
            .map_err(|_| anyhow::anyhow!("library state is locked"))?
            .scan_folder_with_progress(&root_id, &relative_path, |_| {})
            .map(|_| ())
    })
    .await
    .map_err(|error| error.to_string())?
    .map_err(error_message)
}

fn convert_png_folder_recursive(
    folder_path: &Path,
    quality: u8,
) -> anyhow::Result<PngConversionReport> {
    let images = collect_png_images_recursive(folder_path)?;
    let mut converted = 0_u32;
    for image_path in images {
        convert_png_to_jpg_file(&image_path, quality)
            .with_context(|| format!("could not convert {}", image_path.display()))?;
        converted = converted.saturating_add(1);
    }

    Ok(PngConversionReport { converted })
}

fn collect_png_images_recursive(folder_path: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut directories = vec![folder_path.to_path_buf()];
    let mut images = Vec::new();

    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(&directory)
            .with_context(|| format!("could not read directory {}", directory.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                if entry
                    .file_name()
                    .to_string_lossy()
                    .eq_ignore_ascii_case(".picturious")
                {
                    continue;
                }
                directories.push(path);
            } else if file_type.is_file() && is_png_path(&path) {
                images.push(path);
            }
        }
    }

    images.sort_by(compare_paths_alphanumeric);
    Ok(images)
}

fn movie_job_settings(settings: &UiSettings) -> Result<MovieJobSettings, String> {
    let ffmpeg_path = PathBuf::from(settings.ffmpeg_path.trim().trim_matches('"'));
    if !ffmpeg_path.is_file() {
        return Err("ffmpeg.exe is not configured or is no longer available".to_owned());
    }

    let output_folder = settings.movie_output_folder.trim().trim_matches('"');
    let output_folder = if output_folder.is_empty() {
        None
    } else {
        let path = PathBuf::from(output_folder);
        if !path.is_dir() {
            return Err(format!(
                "movie output folder is not available: {}",
                path.display()
            ));
        }
        Some(path)
    };

    let (width, height) = movie_resolution_dimensions(settings)?;
    Ok(MovieJobSettings {
        ffmpeg_path,
        codec: settings.movie_codec,
        quality: settings.movie_quality,
        output_folder,
        width,
        height,
        mode: settings.movie_mode,
        fps: normalize_movie_fps(settings.movie_fps),
        slideshow_seconds: normalize_movie_slideshow_seconds(settings.movie_slideshow_seconds),
    })
}

fn movie_resolution_dimensions(settings: &UiSettings) -> Result<(u32, u32), String> {
    match settings.movie_resolution {
        MovieResolution::P720 => Ok((1280, 720)),
        MovieResolution::P1080 => Ok((1920, 1080)),
        MovieResolution::P4k => Ok((3840, 2160)),
        MovieResolution::Custom => parse_custom_movie_resolution(&settings.movie_custom_resolution),
    }
}

fn parse_custom_movie_resolution(value: &str) -> Result<(u32, u32), String> {
    let parts = value
        .split(|character: char| {
            character == 'x'
                || character == 'X'
                || character == '*'
                || character == ','
                || character.is_ascii_whitespace()
        })
        .filter(|part| !part.trim().is_empty())
        .collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err("custom movie resolution must look like 1920x1080".to_owned());
    }

    let width = parts[0]
        .parse::<u32>()
        .map_err(|_| "custom movie resolution width is invalid".to_owned())?;
    let height = parts[1]
        .parse::<u32>()
        .map_err(|_| "custom movie resolution height is invalid".to_owned())?;
    if width < 16 || height < 16 {
        return Err("custom movie resolution must be at least 16x16".to_owned());
    }
    if width > 8192 || height > 8192 {
        return Err("custom movie resolution cannot exceed 8192x8192".to_owned());
    }

    Ok((even_video_dimension(width), even_video_dimension(height)))
}

fn preview_movie_output(
    folder_path: &Path,
    settings: &MovieJobSettings,
) -> anyhow::Result<MovieOutputPreview> {
    if !folder_path.is_dir() {
        bail!("folder is not available: {}", folder_path.display());
    }

    let images = collect_direct_movie_images(folder_path)?;
    if images.is_empty() {
        bail!("folder has no supported images: {}", folder_path.display());
    }

    let output_path = movie_output_path(folder_path, settings.output_folder.as_deref())?;
    Ok(MovieOutputPreview {
        exists: output_path.exists(),
        output_path: clean_path_string(&output_path),
        image_count: images.len().min(u32::MAX as usize) as u32,
    })
}

fn create_movie_file(
    folder_path: &Path,
    settings: &MovieJobSettings,
    overwrite: bool,
    output_path: &Path,
    app: &AppHandle,
    job_id: &str,
    control: &MovieJobControl,
) -> anyhow::Result<MovieCreationReport> {
    let images = collect_direct_movie_images(folder_path)?;
    if images.is_empty() {
        bail!("folder has no supported images: {}", folder_path.display());
    }
    if output_path.exists() && !overwrite {
        bail!("movie output already exists: {}", output_path.display());
    }

    let mut temp_cleanups = Vec::new();
    let encoder_output_path = temporary_movie_output_path(output_path)?;
    temp_cleanups.push(TempPathCleanup::file(encoder_output_path.clone()));
    emit_movie_output(
        app,
        job_id,
        "status",
        &format!(
            "Creating {} from {} images\n",
            output_path.display(),
            images.len()
        ),
    );
    if control.cancel_requested.load(AtomicOrdering::SeqCst) {
        bail!("movie creation canceled");
    }

    let mut command = Command::new(&settings.ffmpeg_path);
    command
        .arg("-y")
        .arg("-nostdin")
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-stats");

    match settings.mode {
        MovieMode::Movie => {
            let concat_path = temporary_concat_path();
            fs::write(
                &concat_path,
                ffconcat_content(&images, movie_frame_duration_seconds(settings)),
            )
            .with_context(|| format!("could not write {}", concat_path.display()))?;
            command
                .arg("-f")
                .arg("concat")
                .arg("-safe")
                .arg("0")
                .arg("-i")
                .arg(&concat_path)
                .arg("-an")
                .arg("-vf")
                .arg(movie_filter(
                    settings.width,
                    settings.height,
                    movie_output_fps(settings),
                ));
            temp_cleanups.push(TempPathCleanup::file(concat_path));
        }
        MovieMode::Slideshow => {
            let filter_path = temporary_filter_script_path();
            fs::write(&filter_path, slideshow_filter_script(&images, settings))
                .with_context(|| format!("could not write {}", filter_path.display()))?;
            for image in &images {
                command.arg("-i").arg(image);
            }
            command
                .arg("-filter_complex_script")
                .arg(&filter_path)
                .arg("-map")
                .arg("[v]")
                .arg("-an");
            temp_cleanups.push(TempPathCleanup::file(filter_path));
        }
    }

    command
        .arg("-fps_mode")
        .arg("cfr")
        .arg("-r")
        .arg(movie_output_fps(settings).to_string())
        .arg("-c:v")
        .arg(movie_encoder(settings.codec))
        .arg("-preset")
        .arg("medium")
        .arg("-crf")
        .arg(movie_crf(settings.codec, settings.quality))
        .arg("-movflags")
        .arg("+faststart")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if settings.codec == MovieCodec::H265 {
        command
            .arg("-tag:v")
            .arg("hvc1")
            .arg("-x265-params")
            .arg("log-level=error");
    }

    let mut child = match command.arg(&encoder_output_path).spawn() {
        Ok(child) => child,
        Err(error) => {
            return Err(error)
                .with_context(|| format!("could not run {}", settings.ffmpeg_path.display()));
        }
    };
    if let Ok(mut child_id) = control.child_id.lock() {
        *child_id = Some(child.id());
    }
    if control.cancel_requested.load(AtomicOrdering::SeqCst) {
        let _ = terminate_process_tree(child.id());
    }

    let mut readers = Vec::new();
    if let Some(stdout) = child.stdout.take() {
        readers.push(spawn_movie_output_reader(
            stdout,
            app.clone(),
            job_id.to_owned(),
            "stdout",
        ));
    }
    if let Some(stderr) = child.stderr.take() {
        readers.push(spawn_movie_output_reader(
            stderr,
            app.clone(),
            job_id.to_owned(),
            "stderr",
        ));
    }

    let status = child.wait()?;
    if let Ok(mut child_id) = control.child_id.lock() {
        *child_id = None;
    }
    for reader in readers {
        let _ = reader.join();
    }
    if control.cancel_requested.load(AtomicOrdering::SeqCst) {
        bail!("movie creation canceled");
    }
    if !status.success() {
        bail!("ffmpeg failed with {status}");
    }

    replace_file(&encoder_output_path, output_path)?;
    Ok(MovieCreationReport {
        output_path: clean_path_string(&output_path),
        image_count: images.len().min(u32::MAX as usize) as u32,
    })
}

#[allow(clippy::too_many_arguments)]
fn run_movie_creation_job(
    job_id: String,
    folder_path: PathBuf,
    settings: MovieJobSettings,
    overwrite: bool,
    output_path: String,
    image_count: u32,
    control: MovieJobControl,
    app: AppHandle,
    active_movie_jobs: Arc<Mutex<HashMap<String, MovieJobControl>>>,
) {
    let output_path_buf = PathBuf::from(&output_path);
    let result = create_movie_file(
        &folder_path,
        &settings,
        overwrite,
        &output_path_buf,
        &app,
        &job_id,
        &control,
    );
    let canceled = control.cancel_requested.load(AtomicOrdering::SeqCst);
    let finished = match result {
        Ok(report) => MovieCreationFinished {
            job_id: job_id.clone(),
            output_path: report.output_path,
            image_count: report.image_count,
            success: true,
            canceled: false,
            message: "Movie created".to_owned(),
        },
        Err(error) => MovieCreationFinished {
            job_id: job_id.clone(),
            output_path,
            image_count,
            success: false,
            canceled,
            message: if canceled {
                "Movie creation canceled".to_owned()
            } else {
                error_message(error)
            },
        },
    };

    if let Ok(mut active_jobs) = active_movie_jobs.lock() {
        active_jobs.remove(&job_id);
    }
    let _ = app.emit("movie-create-finished", &finished);
}

fn spawn_movie_output_reader<R>(
    mut reader: R,
    app: AppHandle,
    job_id: String,
    stream: &'static str,
) -> thread::JoinHandle<()>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut buffer = [0_u8; 1024];
        loop {
            let count = match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => count,
                Err(_) => break,
            };
            let text = String::from_utf8_lossy(&buffer[..count]).to_string();
            emit_movie_output(&app, &job_id, stream, &text);
        }
    })
}

fn emit_movie_output(app: &AppHandle, job_id: &str, stream: &str, text: &str) {
    let _ = app.emit(
        "movie-create-output",
        &MovieCreationOutput {
            job_id: job_id.to_owned(),
            stream: stream.to_owned(),
            text: text.to_owned(),
        },
    );
}

fn replace_file(source: &Path, destination: &Path) -> anyhow::Result<()> {
    if destination.exists() {
        fs::remove_file(destination)
            .with_context(|| format!("could not replace {}", destination.display()))?;
    }

    match fs::rename(source, destination) {
        Ok(()) => Ok(()),
        Err(rename_error) => {
            fs::copy(source, destination).with_context(|| {
                format!(
                    "could not copy {} to {} after rename failed: {rename_error}",
                    source.display(),
                    destination.display()
                )
            })?;
            fs::remove_file(source)
                .with_context(|| format!("could not remove {}", source.display()))?;
            Ok(())
        }
    }
}

fn sanitize_movie_job_id(job_id: &str) -> Result<String, String> {
    let normalized = job_id.trim();
    if normalized.len() < 8 || normalized.len() > 96 {
        return Err("movie job id is invalid".to_owned());
    }
    if !normalized
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '-' || character == '_')
    {
        return Err("movie job id is invalid".to_owned());
    }
    Ok(normalized.to_owned())
}

#[cfg(windows)]
fn terminate_process_tree(process_id: u32) -> anyhow::Result<()> {
    let status = Command::new("taskkill")
        .arg("/PID")
        .arg(process_id.to_string())
        .arg("/T")
        .arg("/F")
        .status()
        .with_context(|| format!("could not terminate ffmpeg process {process_id}"))?;
    if !status.success() {
        bail!("could not terminate ffmpeg process {process_id}: {status}");
    }
    Ok(())
}

#[cfg(not(windows))]
fn terminate_process_tree(process_id: u32) -> anyhow::Result<()> {
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(process_id.to_string())
        .status()
        .with_context(|| format!("could not terminate ffmpeg process {process_id}"))?;
    if !status.success() {
        bail!("could not terminate ffmpeg process {process_id}: {status}");
    }
    Ok(())
}

fn collect_direct_movie_images(folder_path: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut images = Vec::new();
    for entry in fs::read_dir(folder_path)
        .with_context(|| format!("could not read directory {}", folder_path.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_file() && is_supported_movie_image_path(&path) {
            images.push(path);
        }
    }

    images.sort_by(compare_paths_alphanumeric);
    Ok(images)
}

fn even_video_dimension(value: u32) -> u32 {
    let value = value.max(2);
    if value % 2 == 0 { value } else { value - 1 }
}

fn movie_output_path(folder_path: &Path, output_folder: Option<&Path>) -> anyhow::Result<PathBuf> {
    let folder_name = folder_path
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("movie");
    let output_directory = match output_folder {
        Some(path) => path.to_path_buf(),
        None => folder_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| folder_path.to_path_buf()),
    };

    if !output_directory.is_dir() {
        bail!(
            "movie output folder is not available: {}",
            output_directory.display()
        );
    }

    Ok(output_directory.join(format!("{folder_name}.mp4")))
}

fn temporary_concat_path() -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    std::env::temp_dir().join(format!(
        "picturious-ffmpeg-{}-{millis}.ffconcat",
        std::process::id()
    ))
}

fn temporary_movie_output_path(output_path: &Path) -> anyhow::Result<PathBuf> {
    let parent = output_path
        .parent()
        .with_context(|| format!("movie output has no parent: {}", output_path.display()))?;
    let stem = output_path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("movie");
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    Ok(parent.join(format!(
        ".{stem}.picturious-{}-{millis}.mp4",
        std::process::id()
    )))
}

fn temporary_filter_script_path() -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    std::env::temp_dir().join(format!(
        "picturious-ffmpeg-filter-{}-{millis}.txt",
        std::process::id()
    ))
}

fn ffconcat_content(images: &[PathBuf], duration_seconds: f64) -> String {
    let mut content = String::from("ffconcat version 1.0\n");
    for image in images {
        push_concat_file(&mut content, image);
        content.push_str(&format!("duration {duration_seconds:.6}\n"));
    }
    if let Some(last) = images.last() {
        push_concat_file(&mut content, last);
    }
    content
}

fn push_concat_file(content: &mut String, path: &Path) {
    content.push_str("file ");
    content.push_str(&quote_ffconcat_path(path));
    content.push('\n');
}

fn quote_ffconcat_path(path: &Path) -> String {
    let normalized = clean_path_string(path).replace('\\', "/");
    format!("'{}'", normalized.replace('\'', "'\\''"))
}

fn slideshow_filter_script(images: &[PathBuf], settings: &MovieJobSettings) -> String {
    let fps = movie_output_fps(settings);
    let frame_count = movie_slideshow_frame_count(settings);
    let loop_count = frame_count.saturating_sub(1);
    let mut script = String::new();
    for index in 0..images.len() {
        script.push_str(&format!(
            "[{index}:v]trim=end_frame=1,setpts=PTS-STARTPTS,{}format=yuv420p,loop=loop={loop_count}:size=1:start=0,setpts=N/({fps}*TB)[v{index}];\n",
            movie_base_filter(settings.width, settings.height)
        ));
    }
    for index in 0..images.len() {
        script.push_str(&format!("[v{index}]"));
    }
    script.push_str(&format!(
        "concat=n={}:v=1:a=0,format=yuv420p[v]\n",
        images.len()
    ));
    script
}

fn movie_frame_duration_seconds(settings: &MovieJobSettings) -> f64 {
    match settings.mode {
        MovieMode::Movie => 1.0 / f64::from(settings.fps),
        MovieMode::Slideshow => {
            f64::from(movie_slideshow_frame_count(settings)) / f64::from(movie_output_fps(settings))
        }
    }
}

fn movie_output_fps(settings: &MovieJobSettings) -> u32 {
    match settings.mode {
        MovieMode::Movie => settings.fps,
        MovieMode::Slideshow => 30,
    }
}

fn movie_slideshow_frame_count(settings: &MovieJobSettings) -> u32 {
    let fps = f64::from(movie_output_fps(settings));
    (settings.slideshow_seconds * fps).round().max(1.0) as u32
}

fn movie_filter(width: u32, height: u32, fps: u32) -> String {
    format!(
        "{}fps={fps}:round=near,format=yuv420p",
        movie_base_filter(width, height)
    )
}

fn movie_base_filter(width: u32, height: u32) -> String {
    format!(
        "scale={width}:{height}:force_original_aspect_ratio=decrease:in_range=full:out_range=limited,pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1,"
    )
}

fn movie_encoder(codec: MovieCodec) -> &'static str {
    match codec {
        MovieCodec::H264 => "libx264",
        MovieCodec::H265 => "libx265",
    }
}

fn movie_crf(codec: MovieCodec, quality: MovieQuality) -> &'static str {
    match (codec, quality) {
        (MovieCodec::H264, MovieQuality::High) => "18",
        (MovieCodec::H264, MovieQuality::Balanced) => "23",
        (MovieCodec::H264, MovieQuality::Small) => "28",
        (MovieCodec::H265, MovieQuality::High) => "20",
        (MovieCodec::H265, MovieQuality::Balanced) => "26",
        (MovieCodec::H265, MovieQuality::Small) => "31",
    }
}

fn is_png_path(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.eq_ignore_ascii_case("png"))
        .unwrap_or(false)
}

fn is_supported_movie_image_path(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            [
                "jpg", "jpeg", "png", "webp", "gif", "bmp", "tif", "tiff", "avif",
            ]
            .iter()
            .any(|supported| extension.eq_ignore_ascii_case(supported))
        })
        .unwrap_or(false)
}

fn compare_paths_alphanumeric(left: &PathBuf, right: &PathBuf) -> Ordering {
    let left_name = sortable_path_name(left);
    let right_name = sortable_path_name(right);
    compare_alphanumeric(&left_name, &right_name).then_with(|| left_name.cmp(&right_name))
}

fn sortable_path_name(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(str::to_owned)
        .unwrap_or_else(|| clean_path_string(path))
}

fn compare_alphanumeric(left: &str, right: &str) -> Ordering {
    let left_bytes = left.as_bytes();
    let right_bytes = right.as_bytes();
    let mut left_index = 0;
    let mut right_index = 0;

    while left_index < left_bytes.len() && right_index < right_bytes.len() {
        let left_byte = left_bytes[left_index];
        let right_byte = right_bytes[right_index];
        if left_byte.is_ascii_digit() && right_byte.is_ascii_digit() {
            let ordering = compare_number_segments(left, &mut left_index, right, &mut right_index);
            if ordering != Ordering::Equal {
                return ordering;
            }
            continue;
        }

        let ordering = left_byte
            .to_ascii_lowercase()
            .cmp(&right_byte.to_ascii_lowercase());
        if ordering != Ordering::Equal {
            return ordering;
        }
        left_index += 1;
        right_index += 1;
    }

    left_bytes.len().cmp(&right_bytes.len())
}

fn compare_number_segments(
    left: &str,
    left_index: &mut usize,
    right: &str,
    right_index: &mut usize,
) -> Ordering {
    let left_start = *left_index;
    let right_start = *right_index;
    while *left_index < left.len() && left.as_bytes()[*left_index].is_ascii_digit() {
        *left_index += 1;
    }
    while *right_index < right.len() && right.as_bytes()[*right_index].is_ascii_digit() {
        *right_index += 1;
    }

    let left_digits = &left[left_start..*left_index];
    let right_digits = &right[right_start..*right_index];
    let left_significant = significant_digits(left_digits);
    let right_significant = significant_digits(right_digits);
    left_significant
        .len()
        .cmp(&right_significant.len())
        .then_with(|| left_significant.cmp(right_significant))
        .then_with(|| left_digits.len().cmp(&right_digits.len()))
}

fn significant_digits(digits: &str) -> &str {
    let trimmed = digits.trim_start_matches('0');
    if trimmed.is_empty() { "0" } else { trimmed }
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

fn begin_active_root_job(
    active_scans: &Arc<Mutex<HashSet<String>>>,
    root_id: &str,
    busy_message: &str,
) -> Result<ActiveRootGuard, String> {
    let mut active_scans_lock = active_scans
        .lock()
        .map_err(|_| "scan state is locked".to_owned())?;
    if !active_scans_lock.insert(root_id.to_owned()) {
        return Err(busy_message.to_owned());
    }
    drop(active_scans_lock);

    Ok(ActiveRootGuard {
        active_scans: active_scans.clone(),
        root_id: root_id.to_owned(),
    })
}

fn default_thumb_scale() -> f64 {
    1.0
}

fn default_slideshow_speed_seconds() -> f64 {
    3.0
}

fn default_jpg_quality() -> u8 {
    90
}

fn default_movie_fps() -> u32 {
    30
}

fn default_movie_slideshow_seconds() -> f64 {
    3.0
}

fn default_movie_custom_resolution() -> String {
    "1920x1080".to_owned()
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

fn normalize_jpg_quality(value: u8) -> u8 {
    value.clamp(1, 100)
}

fn normalize_movie_fps(value: u32) -> u32 {
    match value {
        24 | 25 | 30 | 50 | 60 => value,
        _ => default_movie_fps(),
    }
}

fn normalize_movie_slideshow_seconds(value: f64) -> f64 {
    if value.is_finite() && value > 0.0 {
        (value * 1000.0).round() / 1000.0
    } else {
        default_movie_slideshow_seconds()
    }
}

fn clean_settings_path(value: &str) -> String {
    value.trim().trim_matches('"').trim().to_owned()
}

fn normalize_movie_custom_resolution(value: &str) -> String {
    match parse_custom_movie_resolution(value) {
        Ok((width, height)) => format!("{width}x{height}"),
        Err(_) => default_movie_custom_resolution(),
    }
}

fn sanitize_ui_settings(settings: &mut UiSettings) {
    if let Some(window) = settings.window.as_mut() {
        if settings.window_mode == WindowMode::Normal {
            if window.fullscreen {
                settings.window_mode = WindowMode::Fullscreen;
            } else if window.maximized {
                settings.window_mode = WindowMode::Maximized;
            }
        }
        window.fullscreen = false;
        window.maximized = false;
    }
    settings.thumb_scale = clamp_thumb_scale(settings.thumb_scale);
    settings.slideshow_speed_seconds =
        normalize_slideshow_speed_seconds(settings.slideshow_speed_seconds);
    settings.slideshow_ignore_smaller_than =
        normalize_slideshow_ignore_smaller_than(settings.slideshow_ignore_smaller_than);
    settings.jpg_quality = normalize_jpg_quality(settings.jpg_quality);
    settings.ffmpeg_path = clean_settings_path(&settings.ffmpeg_path);
    settings.movie_output_folder = clean_settings_path(&settings.movie_output_folder);
    settings.movie_custom_resolution =
        normalize_movie_custom_resolution(&settings.movie_custom_resolution);
    settings.movie_fps = normalize_movie_fps(settings.movie_fps);
    settings.movie_slideshow_seconds =
        normalize_movie_slideshow_seconds(settings.movie_slideshow_seconds);
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
    let saved = settings
        .window
        .clone()
        .unwrap_or_else(default_window_settings);
    let _ = restore_saved_windowed_state(window, &saved)?;
    match settings.window_mode {
        WindowMode::Normal => {}
        WindowMode::Maximized => {
            window.maximize()?;
        }
        WindowMode::Fullscreen => {
            window.set_fullscreen(true)?;
        }
    }

    Ok(())
}

fn track_window_state(
    window: &WebviewWindow,
    settings: Arc<Mutex<UiSettings>>,
    settings_path: Arc<PathBuf>,
    suppressed_until: Arc<Mutex<Option<Instant>>>,
    closing: Arc<AtomicBool>,
    hdr_viewer: Arc<Mutex<Option<HdrViewerProcess>>>,
) {
    let tracked_window = window.clone();
    window.on_window_event(move |event| {
        if matches!(event, WindowEvent::CloseRequested { .. }) {
            closing.store(true, AtomicOrdering::SeqCst);
            let _ = save_window_close_state(
                &tracked_window,
                &settings,
                settings_path.as_ref().as_path(),
            );
            shutdown_hdr_viewer_process(&hdr_viewer);
            return;
        }

        if matches!(event, WindowEvent::Moved(_) | WindowEvent::Resized(_)) {
            if closing.load(AtomicOrdering::SeqCst)
                || window_state_tracking_is_suppressed(&suppressed_until)
            {
                return;
            }
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
        return set_saved_window_mode(settings, settings_path, WindowMode::Fullscreen);
    }

    if window.is_minimized().unwrap_or(false) {
        return Ok(());
    }

    if window.is_maximized().unwrap_or(false) {
        return set_saved_window_mode(settings, settings_path, WindowMode::Maximized);
    }

    let Some(current) = current_normal_window_state(window)? else {
        return Ok(());
    };

    let mut settings = settings
        .lock()
        .map_err(|_| anyhow::anyhow!("app settings are locked"))?;
    settings.window = Some(current);
    settings.window_mode = WindowMode::Normal;
    write_ui_settings(settings_path, &settings)
}

fn save_window_close_state(
    window: &WebviewWindow,
    settings: &Arc<Mutex<UiSettings>>,
    settings_path: &Path,
) -> anyhow::Result<()> {
    let saved_mode = settings
        .lock()
        .map_err(|_| anyhow::anyhow!("app settings are locked"))?
        .window_mode;

    if window.is_fullscreen().unwrap_or(false) || saved_mode == WindowMode::Fullscreen {
        return set_saved_window_mode(settings, settings_path, WindowMode::Fullscreen);
    }

    if window.is_minimized().unwrap_or(false) {
        return Ok(());
    }

    if window.is_maximized().unwrap_or(false) || saved_mode == WindowMode::Maximized {
        return set_saved_window_mode(settings, settings_path, WindowMode::Maximized);
    }

    save_current_window_state(window, settings, settings_path)
}

fn save_windowed_state_before_fullscreen(
    window: &WebviewWindow,
    settings: &Arc<Mutex<UiSettings>>,
    settings_path: &Path,
) -> anyhow::Result<()> {
    if window.is_fullscreen().unwrap_or(false) || window.is_minimized().unwrap_or(false) {
        return Ok(());
    }

    if window.is_maximized().unwrap_or(false) {
        return set_saved_window_mode(settings, settings_path, WindowMode::Maximized);
    }

    let Some(current) = current_normal_window_state(window)? else {
        return Ok(());
    };
    let mut settings = settings
        .lock()
        .map_err(|_| anyhow::anyhow!("app settings are locked"))?;
    settings.window = Some(current);
    settings.window_mode = WindowMode::Normal;
    write_ui_settings(settings_path, &settings)
}

fn restore_saved_fullscreen_window(
    window: &WebviewWindow,
    settings: &Arc<Mutex<UiSettings>>,
    settings_path: &Path,
) -> anyhow::Result<()> {
    let saved = settings
        .lock()
        .map_err(|_| anyhow::anyhow!("app settings are locked"))?
        .window
        .clone()
        .unwrap_or_else(default_window_settings);
    let mut restored = restore_saved_windowed_state(window, &saved)?.unwrap_or(saved);
    restored.fullscreen = false;
    restored.maximized = false;
    restore_windowed_state_after_fullscreen_transition(window.clone(), restored.clone());
    let mut settings = settings
        .lock()
        .map_err(|_| anyhow::anyhow!("app settings are locked"))?;
    settings.window = Some(restored);
    settings.window_mode = WindowMode::Normal;
    write_ui_settings(settings_path, &settings)
}

fn restore_windowed_state_after_fullscreen_transition(
    window: WebviewWindow,
    saved: WindowSettings,
) {
    thread::spawn(move || {
        for delay in [50, 150, 350, 700] {
            thread::sleep(Duration::from_millis(delay));
            let dispatch_window = window.clone();
            let restore_window = window.clone();
            let saved = saved.clone();
            let _ = dispatch_window.run_on_main_thread(move || {
                if !restore_window.is_fullscreen().unwrap_or(false) {
                    let _ = restore_saved_windowed_state(&restore_window, &saved);
                }
            });
        }
    });
}

fn set_saved_window_mode(
    settings: &Arc<Mutex<UiSettings>>,
    settings_path: &Path,
    mode: WindowMode,
) -> anyhow::Result<()> {
    let mut settings = settings
        .lock()
        .map_err(|_| anyhow::anyhow!("app settings are locked"))?;
    if settings.window.is_none() {
        settings.window = Some(default_window_settings());
    }
    settings.window_mode = mode;
    write_ui_settings(settings_path, &settings)
}

fn current_normal_window_state(window: &WebviewWindow) -> anyhow::Result<Option<WindowSettings>> {
    let position = window.outer_position()?;
    let size = window.inner_size()?;
    if size.width < 320 || size.height < 240 {
        return Ok(None);
    }

    let current = WindowSettings {
        x: position.x,
        y: position.y,
        width: size.width,
        height: size.height,
        maximized: false,
        fullscreen: false,
    };
    if current_window_looks_monitor_sized(window)? {
        return Ok(None);
    }

    Ok(Some(current))
}

fn current_window_looks_monitor_sized(window: &WebviewWindow) -> anyhow::Result<bool> {
    let position = window.outer_position()?;
    let size = window.inner_size()?;
    let current = WindowSettings {
        x: position.x,
        y: position.y,
        width: size.width,
        height: size.height,
        maximized: false,
        fullscreen: false,
    };
    let monitors = window.available_monitors()?;
    Ok(window_state_looks_monitor_sized(&current, &monitors))
}

fn suppress_window_state_tracking(
    suppressed_until: &Arc<Mutex<Option<Instant>>>,
    duration: Duration,
) {
    if let Ok(mut suppressed_until) = suppressed_until.lock() {
        *suppressed_until = Some(Instant::now() + duration);
    }
}

fn window_state_tracking_is_suppressed(suppressed_until: &Arc<Mutex<Option<Instant>>>) -> bool {
    let Ok(mut suppressed_until) = suppressed_until.lock() else {
        return false;
    };
    let Some(until) = *suppressed_until else {
        return false;
    };
    if Instant::now() < until {
        return true;
    }
    *suppressed_until = None;
    false
}

fn default_window_settings() -> WindowSettings {
    WindowSettings {
        x: 0,
        y: 0,
        width: DEFAULT_WINDOW_WIDTH,
        height: DEFAULT_WINDOW_HEIGHT,
        maximized: false,
        fullscreen: false,
    }
}

fn restore_saved_windowed_state(
    window: &WebviewWindow,
    saved: &WindowSettings,
) -> anyhow::Result<Option<WindowSettings>> {
    let monitors = window.available_monitors()?;
    if window.is_maximized().unwrap_or(false) {
        let _ = window.unmaximize();
    }
    let restored = restored_normal_window_state(saved, &monitors);
    if let Some(restored) = restored.as_ref() {
        window.set_size(Size::Physical(PhysicalSize::new(
            restored.width,
            restored.height,
        )))?;
        window.set_position(Position::Physical(PhysicalPosition::new(
            restored.x, restored.y,
        )))?;
    }

    Ok(restored)
}

fn restored_normal_window_state(
    saved: &WindowSettings,
    monitors: &[Monitor],
) -> Option<WindowSettings> {
    let monitor = best_window_monitor(saved, monitors)?;
    let position = monitor.position();
    let size = monitor.size();
    let monitor_left = i64::from(position.x);
    let monitor_top = i64::from(position.y);
    let monitor_width = size.width.max(MIN_RESTORED_WINDOW_WIDTH);
    let monitor_height = size.height.max(MIN_RESTORED_WINDOW_HEIGHT);
    let use_default_bounds = window_state_looks_monitor_sized(saved, monitors);
    let saved_width = if use_default_bounds {
        DEFAULT_WINDOW_WIDTH
    } else {
        saved.width
    };
    let saved_height = if use_default_bounds {
        DEFAULT_WINDOW_HEIGHT
    } else {
        saved.height
    };
    let width = saved_width.clamp(MIN_RESTORED_WINDOW_WIDTH, monitor_width);
    let height = saved_height.clamp(MIN_RESTORED_WINDOW_HEIGHT, monitor_height);
    let max_x = monitor_left + i64::from(monitor_width.saturating_sub(width));
    let max_y = monitor_top + i64::from(monitor_height.saturating_sub(height));
    let x = if use_default_bounds {
        monitor_left + i64::from(monitor_width.saturating_sub(width)) / 2
    } else {
        i64::from(saved.x).clamp(monitor_left, max_x)
    };
    let y = if use_default_bounds {
        monitor_top + i64::from(monitor_height.saturating_sub(height)) / 2
    } else {
        i64::from(saved.y).clamp(monitor_top, max_y)
    };

    Some(WindowSettings {
        x: x as i32,
        y: y as i32,
        width,
        height,
        maximized: false,
        fullscreen: false,
    })
}

fn best_window_monitor<'a>(
    window: &WindowSettings,
    monitors: &'a [Monitor],
) -> Option<&'a Monitor> {
    monitors
        .iter()
        .filter_map(|monitor| {
            let intersection = window_monitor_intersection_area(window, monitor);
            (intersection > 0).then_some((intersection, monitor))
        })
        .max_by_key(|(intersection, _)| *intersection)
        .map(|(_, monitor)| monitor)
        .or_else(|| monitors.first())
}

fn window_state_looks_monitor_sized(window: &WindowSettings, monitors: &[Monitor]) -> bool {
    best_window_monitor(window, monitors).is_some_and(|monitor| {
        let size = monitor.size();
        window.width.saturating_add(MONITOR_SIZED_WINDOW_TOLERANCE) >= size.width
            && window.height.saturating_add(MONITOR_SIZED_WINDOW_TOLERANCE) >= size.height
    })
}

fn window_monitor_intersection_area(window: &WindowSettings, monitor: &Monitor) -> i64 {
    let window_left = i64::from(window.x);
    let window_top = i64::from(window.y);
    let window_right = window_left + i64::from(window.width);
    let window_bottom = window_top + i64::from(window.height);
    let position = monitor.position();
    let size = monitor.size();
    let monitor_left = i64::from(position.x);
    let monitor_top = i64::from(position.y);
    let monitor_right = monitor_left + i64::from(size.width);
    let monitor_bottom = monitor_top + i64::from(size.height);

    let intersection_width = window_right.min(monitor_right) - window_left.max(monitor_left);
    let intersection_height = window_bottom.min(monitor_bottom) - window_top.max(monitor_top);
    if intersection_width < 80 || intersection_height < 80 {
        return 0;
    }

    intersection_width * intersection_height
}

fn is_supported_splat_path(path: &Path) -> bool {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if file_name.ends_with(".compressed.ply")
        || file_name.ends_with(".meta.json")
        || file_name.ends_with(".lod-meta.json")
    {
        return true;
    }

    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            ["spz", "sog", "ply", "splat", "ksplat", "rad"]
                .iter()
                .any(|supported| extension.eq_ignore_ascii_case(supported))
        })
        .unwrap_or(false)
}

fn is_supported_model_path(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.eq_ignore_ascii_case("glb"))
        .unwrap_or(false)
}

fn is_supported_3d_asset_path(path: &Path) -> bool {
    is_supported_splat_path(path) || is_supported_model_path(path)
}

fn decode_image_data_url(data_url: &str) -> Result<(String, Vec<u8>), String> {
    let (header, encoded) = data_url
        .split_once(',')
        .ok_or_else(|| "thumbnail image data is not a data URL".to_owned())?;
    let header = header.to_ascii_lowercase();
    let mime_type = if header.starts_with("data:image/jpeg;base64") {
        "image/jpeg"
    } else if header.starts_with("data:image/png;base64") {
        "image/png"
    } else {
        return Err("thumbnail image data must be JPEG or PNG".to_owned());
    };

    let bytes = STANDARD
        .decode(encoded)
        .map_err(|error| format!("could not decode thumbnail image data: {error}"))?;
    Ok((mime_type.to_owned(), bytes))
}

fn image_data_url(mime_type: &str, bytes: &[u8]) -> String {
    format!("data:{mime_type};base64,{}", STANDARD.encode(bytes))
}

fn splat_placeholder_data_url() -> String {
    static PLACEHOLDER: OnceLock<String> = OnceLock::new();
    PLACEHOLDER
        .get_or_init(build_splat_placeholder_data_url)
        .clone()
}

fn build_splat_placeholder_data_url() -> String {
    let mut dots = String::new();
    append_splat_face_dots(
        &mut dots,
        [
            (130.0, 162.0),
            (256.0, 235.0),
            (256.0, 382.0),
            (130.0, 310.0),
        ],
        "#23645d",
        3.7,
        0.58,
    );
    append_splat_face_dots(
        &mut dots,
        [
            (256.0, 235.0),
            (382.0, 162.0),
            (382.0, 310.0),
            (256.0, 382.0),
        ],
        "#2f7b72",
        3.7,
        0.62,
    );
    append_splat_face_dots(
        &mut dots,
        [
            (256.0, 90.0),
            (382.0, 162.0),
            (256.0, 235.0),
            (130.0, 162.0),
        ],
        "#d8a35d",
        3.9,
        0.74,
    );

    let svg = format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="512" height="512" viewBox="0 0 512 512">
<defs>
<linearGradient id="top" x1="160" y1="100" x2="340" y2="220" gradientUnits="userSpaceOnUse">
<stop offset="0" stop-color="#f3e4c5"/>
<stop offset="1" stop-color="#d7a15b"/>
</linearGradient>
<linearGradient id="left" x1="126" y1="172" x2="270" y2="382" gradientUnits="userSpaceOnUse">
<stop offset="0" stop-color="#d7e9e4"/>
<stop offset="1" stop-color="#8ebdb2"/>
</linearGradient>
<linearGradient id="right" x1="250" y1="190" x2="390" y2="350" gradientUnits="userSpaceOnUse">
<stop offset="0" stop-color="#c7ddd8"/>
<stop offset="1" stop-color="#4f9187"/>
</linearGradient>
<filter id="shadow" x="72" y="58" width="368" height="360" filterUnits="userSpaceOnUse">
<feDropShadow dx="0" dy="12" stdDeviation="14" flood-color="#172321" flood-opacity="0.18"/>
</filter>
</defs>
<rect width="512" height="512" rx="48" fill="#f6f7f4"/>
<rect x="42" y="42" width="428" height="428" rx="34" fill="#ffffff" stroke="#d5dcd9" stroke-width="8"/>
<rect x="64" y="64" width="384" height="384" rx="24" fill="#eef2f0" stroke="#e3e8e5" stroke-width="2"/>
<g filter="url(#shadow)">
<polygon points="256,90 382,162 256,235 130,162" fill="url(#top)" opacity="0.72"/>
<polygon points="130,162 256,235 256,382 130,310" fill="url(#left)" opacity="0.82"/>
<polygon points="256,235 382,162 382,310 256,382" fill="url(#right)" opacity="0.86"/>
</g>
<g>{dots}</g>
<g fill="none" stroke-linecap="round" stroke-linejoin="round">
<path d="M256 90 382 162 382 310 256 382 130 310 130 162 256 90Z" stroke="#205e57" stroke-width="10"/>
<path d="M130 162 256 235 382 162" stroke="#2b7068" stroke-width="8"/>
<path d="M256 235V382" stroke="#2b7068" stroke-width="8"/>
<path d="M256 90 256 235" stroke="#e1b26d" stroke-width="5" opacity="0.55"/>
</g>
<g fill="#f0c276" opacity="0.95">
<circle cx="256" cy="90" r="7"/>
<circle cx="130" cy="162" r="6"/>
<circle cx="382" cy="162" r="6"/>
</g>
</svg>"##
    );
    format!("data:image/svg+xml;base64,{}", STANDARD.encode(svg))
}

fn model_placeholder_data_url() -> String {
    static PLACEHOLDER: OnceLock<String> = OnceLock::new();
    PLACEHOLDER
        .get_or_init(build_model_placeholder_data_url)
        .clone()
}

fn build_model_placeholder_data_url() -> String {
    let svg = r##"<svg xmlns="http://www.w3.org/2000/svg" width="512" height="512" viewBox="0 0 512 512">
<defs>
<linearGradient id="bg" x1="42" y1="42" x2="470" y2="470" gradientUnits="userSpaceOnUse">
<stop offset="0" stop-color="#f8f6ef"/>
<stop offset="1" stop-color="#e3ebe6"/>
</linearGradient>
<linearGradient id="top" x1="152" y1="104" x2="360" y2="232" gradientUnits="userSpaceOnUse">
<stop offset="0" stop-color="#f1d9a6"/>
<stop offset="1" stop-color="#c99b58"/>
</linearGradient>
<linearGradient id="left" x1="126" y1="176" x2="260" y2="380" gradientUnits="userSpaceOnUse">
<stop offset="0" stop-color="#b8d2cc"/>
<stop offset="1" stop-color="#5f9b90"/>
</linearGradient>
<linearGradient id="right" x1="252" y1="182" x2="388" y2="356" gradientUnits="userSpaceOnUse">
<stop offset="0" stop-color="#8fbab1"/>
<stop offset="1" stop-color="#2f7068"/>
</linearGradient>
<filter id="shadow" x="72" y="68" width="368" height="360" filterUnits="userSpaceOnUse">
<feDropShadow dx="0" dy="14" stdDeviation="15" flood-color="#1d2523" flood-opacity="0.18"/>
</filter>
</defs>
<rect width="512" height="512" rx="48" fill="url(#bg)"/>
<rect x="42" y="42" width="428" height="428" rx="34" fill="#ffffff" stroke="#d5dcd7" stroke-width="8"/>
<rect x="66" y="66" width="380" height="380" rx="24" fill="#f0f4f1" stroke="#e5e9e5" stroke-width="2"/>
<g filter="url(#shadow)">
<polygon points="256,95 382,168 256,240 130,168" fill="url(#top)"/>
<polygon points="130,168 256,240 256,382 130,310" fill="url(#left)"/>
<polygon points="256,240 382,168 382,310 256,382" fill="url(#right)"/>
</g>
<g fill="none" stroke-linecap="round" stroke-linejoin="round">
<path d="M256 95 382 168 382 310 256 382 130 310 130 168 256 95Z" stroke="#244f4a" stroke-width="10"/>
<path d="M130 168 256 240 382 168" stroke="#2f675f" stroke-width="8"/>
<path d="M256 240V382" stroke="#2f675f" stroke-width="8"/>
<path d="M174 194 300 121M211 216 337 144M172 332 172 192M214 356 214 216M300 356 300 216M342 332 342 192" stroke="#ffffff" stroke-width="4" opacity="0.34"/>
</g>
<g fill="#244f4a" opacity="0.95">
<circle cx="256" cy="95" r="6"/>
<circle cx="130" cy="168" r="5"/>
<circle cx="382" cy="168" r="5"/>
<circle cx="256" cy="240" r="5"/>
<circle cx="256" cy="382" r="6"/>
</g>
</svg>"##;
    format!("data:image/svg+xml;base64,{}", STANDARD.encode(svg))
}

fn append_splat_face_dots(
    output: &mut String,
    corners: [(f64, f64); 4],
    color: &str,
    radius: f64,
    opacity: f64,
) {
    const DOTS_PER_AXIS: usize = 8;
    for row in 0..DOTS_PER_AXIS {
        for column in 0..DOTS_PER_AXIS {
            let u = (column as f64 + 0.5) / DOTS_PER_AXIS as f64;
            let v = (row as f64 + 0.5) / DOTS_PER_AXIS as f64;
            let x = (1.0 - u) * (1.0 - v) * corners[0].0
                + u * (1.0 - v) * corners[1].0
                + u * v * corners[2].0
                + (1.0 - u) * v * corners[3].0;
            let y = (1.0 - u) * (1.0 - v) * corners[0].1
                + u * (1.0 - v) * corners[1].1
                + u * v * corners[2].1
                + (1.0 - u) * v * corners[3].1;
            let edge_fade = 0.82 + 0.18 * (1.0 - (u - 0.5).abs() * 1.4).max(0.0);
            let _ = write!(
                output,
                r#"<circle cx="{x:.1}" cy="{y:.1}" r="{radius:.1}" fill="{color}" opacity="{:.2}"/>"#,
                opacity * edge_fade
            );
        }
    }
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

fn folder_path_for(
    library: &Arc<Mutex<LibraryManager>>,
    root_id: &str,
    relative_path: &str,
) -> Result<PathBuf, String> {
    library
        .lock()
        .map_err(|_| "library state is locked".to_owned())?
        .folder_path(root_id, relative_path)
        .map_err(error_message)
}

#[cfg(windows)]
fn show_in_explorer(path: &Path) -> anyhow::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::UI::Shell::{ILCreateFromPathW, ILFree, SHOpenFolderAndSelectItems};

    if !path.exists() {
        anyhow::bail!("path is not available: {}", path.display());
    }

    let wide_path = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let item = unsafe { ILCreateFromPathW(wide_path.as_ptr()) };
    if item.is_null() {
        anyhow::bail!("could not create Explorer item for {}", path.display());
    }

    let result = unsafe { SHOpenFolderAndSelectItems(item, 0, std::ptr::null(), 0) };
    unsafe { ILFree(item) };

    if result < 0 {
        anyhow::bail!(
            "could not select {} in Explorer (shell error {:#010x})",
            path.display(),
            result as u32
        );
    }

    Ok(())
}

#[cfg(not(windows))]
fn show_in_explorer(path: &Path) -> anyhow::Result<()> {
    anyhow::bail!(
        "show in Explorer is only implemented on Windows for {}",
        path.display()
    )
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
fn open_path_in_default_app(path: &Path) -> anyhow::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::UI::Shell::ShellExecuteW;

    if !path.exists() {
        anyhow::bail!("path is not available: {}", path.display());
    }

    let wide_path = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let result = unsafe {
        ShellExecuteW(
            std::ptr::null_mut(),
            std::ptr::null(),
            wide_path.as_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            1,
        ) as isize
    };

    if result <= 32 {
        anyhow::bail!("could not open {} in the default app", path.display());
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn open_path_in_default_app(path: &Path) -> anyhow::Result<()> {
    Command::new("open")
        .arg(path)
        .spawn()
        .with_context(|| format!("could not open {} in the default app", path.display()))?;
    Ok(())
}

#[cfg(all(not(windows), not(target_os = "macos")))]
fn open_path_in_default_app(path: &Path) -> anyhow::Result<()> {
    Command::new("xdg-open")
        .arg(path)
        .spawn()
        .with_context(|| format!("could not open {} in the default app", path.display()))?;
    Ok(())
}

#[cfg(windows)]
fn open_url_in_default_browser(url: &str) -> anyhow::Result<()> {
    use windows_sys::Win32::UI::Shell::ShellExecuteW;

    let wide_url = url
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let result = unsafe {
        ShellExecuteW(
            std::ptr::null_mut(),
            std::ptr::null(),
            wide_url.as_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            1,
        ) as isize
    };

    if result <= 32 {
        anyhow::bail!("could not open homepage in the default browser");
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn open_url_in_default_browser(url: &str) -> anyhow::Result<()> {
    Command::new("open")
        .arg(url)
        .spawn()
        .with_context(|| format!("could not open {url} in the default browser"))?;
    Ok(())
}

#[cfg(all(not(windows), not(target_os = "macos")))]
fn open_url_in_default_browser(url: &str) -> anyhow::Result<()> {
    Command::new("xdg-open")
        .arg(url)
        .spawn()
        .with_context(|| format!("could not open {url} in the default browser"))?;
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

async fn spawn_image_preview_job(path: PathBuf, size: u32) -> Result<GeneratedThumbnail, String> {
    tauri::async_runtime::spawn_blocking(move || {
        generate_image_preview(&path, size).map_err(|error| {
            anyhow::anyhow!("could not generate preview for {}: {error}", path.display())
        })
    })
    .await
    .map_err(|error| error.to_string())?
    .map_err(error_message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_window_flags_migrate_to_window_mode() {
        let mut settings: UiSettings = serde_json::from_value(serde_json::json!({
            "window": {
                "x": 25,
                "y": 50,
                "width": 900,
                "height": 700,
                "fullscreen": true
            }
        }))
        .unwrap();

        sanitize_ui_settings(&mut settings);

        assert_eq!(settings.window_mode, WindowMode::Fullscreen);
        let window = settings.window.unwrap();
        assert_eq!(
            (window.x, window.y, window.width, window.height),
            (25, 50, 900, 700)
        );
        assert!(!window.fullscreen);
        assert!(!window.maximized);
    }

    #[test]
    fn window_mode_serializes_separately_from_window_bounds() {
        let mut settings = UiSettings::default();
        settings.window = Some(WindowSettings {
            x: 25,
            y: 50,
            width: 900,
            height: 700,
            maximized: true,
            fullscreen: true,
        });
        settings.window_mode = WindowMode::Fullscreen;

        let value = serde_json::to_value(&settings).unwrap();

        assert_eq!(value["window_mode"], "fullscreen");
        assert_eq!(value["window"]["width"], 900);
        assert!(value["window"].get("fullscreen").is_none());
        assert!(value["window"].get("maximized").is_none());
    }

    #[test]
    fn hdr_viewer_logging_is_disabled_by_default() {
        let settings_path = Path::new("C:/tmp/picturious/settings.json");

        assert_eq!(
            hdr_viewer_log_path_for_settings(settings_path, None, None),
            None
        );
    }

    #[test]
    fn hdr_viewer_logging_can_be_enabled_explicitly() {
        let settings_path = Path::new("C:/tmp/picturious/settings.json");

        assert_eq!(
            hdr_viewer_log_path_for_settings(
                settings_path,
                None,
                Some(std::ffi::OsStr::new("true"))
            ),
            Some(PathBuf::from("C:/tmp/picturious/hdr-viewer.log"))
        );
        assert_eq!(
            hdr_viewer_log_path_for_settings(
                settings_path,
                Some(std::ffi::OsStr::new("C:/tmp/custom-hdr-viewer.log")),
                None
            ),
            Some(PathBuf::from("C:/tmp/custom-hdr-viewer.log"))
        );
    }

    #[test]
    fn hdr_viewer_logging_false_env_disables_logging() {
        let settings_path = Path::new("C:/tmp/picturious/settings.json");

        assert_eq!(
            hdr_viewer_log_path_for_settings(
                settings_path,
                Some(std::ffi::OsStr::new("false")),
                Some(std::ffi::OsStr::new("true"))
            ),
            None
        );
    }
}
