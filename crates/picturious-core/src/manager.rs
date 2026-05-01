use crate::db::{
    RootDatabase, clean_path_string, root_database_exists, root_database_path, root_display_name,
    validate_root_path,
};
use crate::models::{
    FolderView, ImageSummary, LibraryOverview, LibraryRoot, ScanProgress, ScanReport,
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct AppConfig {
    roots: Vec<KnownRoot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct KnownRoot {
    id: String,
    path: String,
    display_name: String,
}

#[derive(Debug, Clone)]
pub struct ScanTarget {
    pub root_id: String,
    pub path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct FolderViewTarget {
    pub root_id: String,
    pub path: PathBuf,
    pub display_name: String,
}

pub struct LibraryManager {
    config_path: PathBuf,
    roots: Vec<KnownRoot>,
}

impl LibraryManager {
    pub fn new(config_dir: impl AsRef<Path>) -> Result<Self> {
        let config_dir = config_dir.as_ref();
        fs::create_dir_all(config_dir)
            .with_context(|| format!("could not create {}", config_dir.display()))?;
        let config_path = config_dir.join("roots.json");
        let roots = read_config(&config_path).unwrap_or_default().roots;

        Ok(Self { config_path, roots })
    }

    pub fn overview(&self) -> Result<LibraryOverview> {
        let roots = self
            .roots
            .iter()
            .map(|root| self.library_root(root))
            .collect::<Result<Vec<_>>>()?;
        let mut roots = roots;
        roots.sort_by(|left, right| {
            left.display_name
                .to_lowercase()
                .cmp(&right.display_name.to_lowercase())
                .then_with(|| left.path.to_lowercase().cmp(&right.path.to_lowercase()))
        });

        Ok(LibraryOverview { roots })
    }

    pub fn add_root(&mut self, path: &str) -> Result<LibraryRoot> {
        let root_path = validate_root_path(path)?;
        let db = RootDatabase::open(&root_path)?;
        let root_id = db.root_id()?;
        let display_name = root_display_name(&root_path);
        let path = clean_path_string(&root_path);

        let saved_root_id = root_id.clone();
        let saved_path = path.clone();

        if let Some(existing) = self
            .roots
            .iter_mut()
            .find(|known| known.id == root_id || same_path(&known.path, &path))
        {
            existing.id = root_id;
            existing.path = path;
            existing.display_name = display_name;
        } else {
            self.roots.push(KnownRoot {
                id: root_id,
                path,
                display_name,
            });
        }

        self.save_config()?;
        let root = self
            .roots
            .iter()
            .find(|root| root.id == saved_root_id || root.path == saved_path)
            .context("root was not saved")?;
        self.library_root(root)
    }

    pub fn scan_root(&mut self, root_id: &str) -> Result<ScanReport> {
        self.scan_root_with_progress(root_id, |_| {})
    }

    pub fn scan_root_with_progress<F>(&self, root_id: &str, on_progress: F) -> Result<ScanReport>
    where
        F: FnMut(ScanProgress),
    {
        let known_root = self.known_root(root_id)?;
        let mut db = self.open_connected_database(known_root)?;
        db.scan_with_progress(root_id, on_progress)
    }

    pub fn scan_target(&self, root_id: &str) -> Result<ScanTarget> {
        let known_root = self.known_root(root_id)?;
        self.open_connected_database(known_root)?;
        Ok(ScanTarget {
            root_id: known_root.id.clone(),
            path: PathBuf::from(&known_root.path),
        })
    }

    pub fn folder_view_target(&self, root_id: &str) -> Result<FolderViewTarget> {
        let known_root = self.known_root(root_id)?;
        let path = PathBuf::from(&known_root.path);
        if !path.is_dir() {
            bail!("root is not connected: {}", known_root.path);
        }

        Ok(FolderViewTarget {
            root_id: known_root.id.clone(),
            path,
            display_name: known_root.display_name.clone(),
        })
    }

    pub fn remove_root(&mut self, root_id: &str) -> Result<()> {
        let before = self.roots.len();
        self.roots.retain(|root| root.id != root_id);
        if self.roots.len() == before {
            bail!("unknown root: {root_id}");
        }

        self.save_config()
    }

    pub fn folder_view(&self, root_id: &str, relative_path: &str) -> Result<FolderView> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.folder_view(root_id, &known_root.display_name, relative_path)
    }

    pub fn image_path(&self, root_id: &str, image_id: i64) -> Result<(PathBuf, i64)> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.image_path(image_id)
    }

    pub fn recursive_images_for_folder(
        &self,
        root_id: &str,
        relative_path: &str,
    ) -> Result<Vec<ImageSummary>> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.recursive_images_for_folder(root_id, relative_path)
    }

    pub fn refresh_image_metadata(&self, root_id: &str, image_id: i64) -> Result<()> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.refresh_image_metadata(image_id)
    }

    pub fn delete_image(&self, root_id: &str, image_id: i64) -> Result<()> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.delete_image(image_id)
    }

    pub fn set_folder_thumbnail(
        &mut self,
        root_id: &str,
        folder_id: i64,
        image_id: i64,
    ) -> Result<()> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.set_folder_thumbnail(folder_id, image_id)
    }

    fn known_root(&self, root_id: &str) -> Result<&KnownRoot> {
        self.roots
            .iter()
            .find(|root| root.id == root_id)
            .with_context(|| format!("unknown root: {root_id}"))
    }

    fn open_connected_database(&self, root: &KnownRoot) -> Result<RootDatabase> {
        let root_path = PathBuf::from(&root.path);
        if !root_path.is_dir() {
            bail!("root is not connected: {}", root.path);
        }
        let db = RootDatabase::open_existing(&root_path)?
            .with_context(|| format!("root database is missing: {}", root.path))?;
        let database_root_id = db.root_id()?;
        if database_root_id != root.id {
            bail!(
                "root database id does not match the configured root id for {}",
                root.path
            );
        }
        Ok(db)
    }

    fn library_root(&self, root: &KnownRoot) -> Result<LibraryRoot> {
        let root_path = PathBuf::from(&root.path);
        let database_path = root_database_path(&root_path);
        let connected_candidate = root_path.is_dir() && root_database_exists(&root_path);
        let (connected, folder_count, image_count, thumbnail_image_id) = if connected_candidate {
            if let Some(db) = RootDatabase::open_existing(&root_path)? {
                if db.root_id()? == root.id {
                    let (folder_count, image_count) = db.stats()?;
                    let thumbnail_image_id = db.root_thumbnail_image_id()?;
                    (true, folder_count, image_count, thumbnail_image_id)
                } else {
                    (false, 0, 0, None)
                }
            } else {
                (false, 0, 0, None)
            }
        } else {
            (false, 0, 0, None)
        };

        Ok(LibraryRoot {
            id: root.id.clone(),
            display_name: root.display_name.clone(),
            path: root.path.clone(),
            connected,
            database_path: connected.then(|| clean_path_string(&database_path)),
            folder_count,
            image_count,
            thumbnail_image_id,
        })
    }

    fn save_config(&self) -> Result<()> {
        let config = AppConfig {
            roots: self.roots.clone(),
        };
        let json = serde_json::to_string_pretty(&config)?;
        fs::write(&self.config_path, json)
            .with_context(|| format!("could not write {}", self.config_path.display()))?;
        Ok(())
    }
}

fn read_config(path: &Path) -> Result<AppConfig> {
    if !path.is_file() {
        return Ok(AppConfig::default());
    }

    let contents =
        fs::read_to_string(path).with_context(|| format!("could not read {}", path.display()))?;
    Ok(serde_json::from_str(&contents).unwrap_or_default())
}

fn same_path(left: &str, right: &str) -> bool {
    left.trim_end_matches(['\\', '/'])
        .eq_ignore_ascii_case(right.trim_end_matches(['\\', '/']))
}
