use crate::models::{
    FolderMetadata, FolderSummary, FolderView, FolderViewHeader, ImageMetadata, ImageSummary,
    MetadataCombineMode, MetadataPersonSummary, MetadataSearchQuery, MetadataTag, ScanProgress,
    ScanReport,
};
use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

const DB_DIR: &str = ".picturious";
const DB_FILE: &str = "root.sqlite";
const ROOT_RELATIVE_PATH: &str = "";
const SCHEMA_VERSION: &str = "6";
const FOLDER_VIEW_BATCH_SIZE: usize = 64;
const SUPPORTED_IMAGE_EXTENSIONS: &[&str] = &[
    "jpg", "jpeg", "png", "webp", "gif", "bmp", "tif", "tiff", "avif", "heic", "heif", "hif",
];
const SUPPORTED_SPLAT_EXTENSIONS: &[&str] = &["spz", "sog", "ply", "splat", "ksplat", "rad"];
const SUPPORTED_MODEL_EXTENSIONS: &[&str] = &["glb"];
#[cfg(windows)]
const FILE_ATTRIBUTE_HIDDEN: u32 = 0x2;
#[cfg(windows)]
const FILE_ATTRIBUTE_SYSTEM: u32 = 0x4;

pub fn root_database_path(root_path: &Path) -> PathBuf {
    root_path.join(DB_DIR).join(DB_FILE)
}

pub fn root_database_exists(root_path: &Path) -> bool {
    root_database_path(root_path).is_file()
}

pub struct RootDatabase {
    root_path: PathBuf,
    connection: Connection,
}

struct ScannedImage {
    file_name: String,
    relative_path: String,
    file_size: u64,
    modified_unix_ms: i64,
}

struct ScannedFolder {
    file_name: String,
    relative_path: String,
    modified_unix_ms: i64,
}

struct DirectorySnapshot {
    child_folders: Vec<ScannedFolder>,
    images: Vec<ScannedImage>,
    content_hash: String,
    skipped_entries: u32,
}

struct ExistingImage {
    file_name: String,
    relative_path: String,
    file_size: u64,
    modified_unix_ms: i64,
}

pub struct StoredSplatThumbnail {
    pub mime_type: String,
    pub data: Vec<u8>,
    pub camera_json: Option<String>,
}

struct FolderRow {
    id: i64,
    relative_path: String,
    parent_relative_path: Option<String>,
    selected_thumbnail_image_id: Option<i64>,
    image_count: u32,
    child_folder_count: u32,
    validated: bool,
}

struct FolderValidation {
    relative_path: String,
    changed: bool,
    child_relative_paths: Vec<String>,
    image_count: u32,
    skipped_entries: u32,
}

impl RootDatabase {
    pub fn open(root_path: impl AsRef<Path>) -> Result<Self> {
        Self::connect(root_path, true)
    }

    pub fn open_existing(root_path: impl AsRef<Path>) -> Result<Option<Self>> {
        let root_path = root_path.as_ref();
        if !root_path.is_dir() || !root_database_exists(root_path) {
            return Ok(None);
        }

        Ok(Some(Self::connect(root_path, false)?))
    }

    fn connect(root_path: impl AsRef<Path>, initialize: bool) -> Result<Self> {
        let root_path = root_path.as_ref().to_path_buf();
        if !root_path.is_dir() {
            bail!("root path is not a directory: {}", root_path.display());
        }

        if initialize {
            let app_dir = root_path.join(DB_DIR);
            fs::create_dir_all(&app_dir)
                .with_context(|| format!("could not create {}", app_dir.display()))?;
        } else if !root_database_exists(&root_path) {
            bail!("root database does not exist below {}", root_path.display());
        }

        let connection = Connection::open(root_database_path(&root_path))
            .with_context(|| format!("could not open database below {}", root_path.display()))?;

        let db = Self {
            root_path,
            connection,
        };
        db.configure()?;
        db.init_schema()?;
        Ok(db)
    }

    pub fn root_id(&self) -> Result<String> {
        let existing = self.meta_value("root_id")?;
        if let Some(root_id) = existing {
            return Ok(root_id);
        }

        let root_id = Uuid::new_v4().to_string();
        self.set_meta_value("root_id", &root_id)?;
        Ok(root_id)
    }

    pub fn stats(&self) -> Result<(u32, u32)> {
        let folder_count = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM folders WHERE relative_path <> ''",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count.max(0) as u32)
            .context("could not count folders")?;
        let image_count = self
            .connection
            .query_row("SELECT COUNT(*) FROM images", [], |row| {
                row.get::<_, i64>(0)
            })
            .map(|count| count.max(0) as u32)
            .context("could not count images")?;

        Ok((folder_count, image_count))
    }

    pub fn root_thumbnail_image_id(&self) -> Result<Option<i64>> {
        let (folder_id, selected_thumbnail_image_id): (i64, Option<i64>) =
            self.connection.query_row(
                "SELECT id, selected_thumbnail_image_id FROM folders WHERE relative_path = ''",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;

        self.thumbnail_image_id(folder_id, ROOT_RELATIVE_PATH, selected_thumbnail_image_id)
    }

    pub fn scan(&mut self, root_id: &str) -> Result<ScanReport> {
        self.scan_with_progress(root_id, |_| {})
    }

    pub fn scan_with_progress<F>(&mut self, root_id: &str, mut on_progress: F) -> Result<ScanReport>
    where
        F: FnMut(ScanProgress),
    {
        self.rescan_with_progress(root_id, ROOT_RELATIVE_PATH, |progress| {
            on_progress(progress)
        })
    }

    pub fn rescan_with_progress<F>(
        &mut self,
        root_id: &str,
        relative_path: &str,
        mut on_progress: F,
    ) -> Result<ScanReport>
    where
        F: FnMut(ScanProgress),
    {
        let start_relative_path = normalize_relative_path(relative_path);
        let mut folders_seen = 0_u32;
        let mut images_seen = 0_u32;
        let mut skipped_entries = 0_u32;
        let mut pending_dirs = VecDeque::from([start_relative_path]);
        let mut queued_dirs = HashSet::new();

        while let Some(relative_path) = pending_dirs.pop_front() {
            if !queued_dirs.insert(relative_path.clone()) {
                continue;
            }

            let validation = self.validate_folder(root_id, &relative_path)?;
            folders_seen += 1;
            images_seen = images_seen.saturating_add(validation.image_count);
            skipped_entries = skipped_entries.saturating_add(validation.skipped_entries);

            // Explicit rescans still walk the subtree. Parent directory timestamps
            // are not reliable enough to prove that deeper descendants are unchanged.
            for child_relative_path in &validation.child_relative_paths {
                pending_dirs.push_back(child_relative_path.clone());
            }

            on_progress(ScanProgress {
                root_id: root_id.to_owned(),
                folders_seen,
                images_seen,
                skipped_entries,
                current_relative_path: validation.relative_path,
                changed: validation.changed,
            });
        }

        Ok(ScanReport {
            root_id: root_id.to_owned(),
            folders_seen,
            images_seen,
            skipped_entries,
        })
    }

    pub fn folder_view(
        &self,
        root_id: &str,
        root_display_name: &str,
        relative_path: &str,
    ) -> Result<FolderView> {
        let header = self.folder_view_header(root_id, root_display_name, relative_path)?;
        let folders = self.child_folders(root_id, &header.relative_path)?;
        let images = self.images_for_folder(root_id, &header.relative_path)?;

        Ok(FolderView {
            root_id: header.root_id,
            root_display_name: header.root_display_name,
            folder_id: header.folder_id,
            relative_path: header.relative_path,
            parent_relative_path: header.parent_relative_path,
            folders,
            images,
        })
    }

    pub fn folder_view_header(
        &self,
        root_id: &str,
        root_display_name: &str,
        relative_path: &str,
    ) -> Result<FolderViewHeader> {
        let normalized_relative_path = normalize_relative_path(relative_path);
        let parent = if normalized_relative_path == ROOT_RELATIVE_PATH {
            None
        } else {
            self.visible_parent_relative_path(&normalized_relative_path)?
        };

        let folder_id = self
            .connection
            .query_row(
                "SELECT id FROM folders WHERE relative_path = ?1",
                params![normalized_relative_path],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .with_context(|| format!("folder is not indexed: {normalized_relative_path}"))?;

        Ok(FolderViewHeader {
            root_id: root_id.to_owned(),
            root_display_name: root_display_name.to_owned(),
            folder_id,
            relative_path: normalized_relative_path,
            parent_relative_path: parent,
        })
    }

    pub fn stream_folder_view_items<F>(
        &self,
        root_id: &str,
        relative_path: &str,
        mut on_batch: F,
    ) -> Result<()>
    where
        F: FnMut(Vec<FolderSummary>, Vec<ImageSummary>) -> Result<()>,
    {
        let normalized_relative_path = normalize_relative_path(relative_path);
        let mut folder_batch = Vec::with_capacity(FOLDER_VIEW_BATCH_SIZE);
        self.for_each_direct_child_folder_row(&normalized_relative_path, |row| {
            for visible_row in self.visible_folder_rows_from(row, 1)? {
                folder_batch.push(self.folder_summary(
                    root_id,
                    &normalized_relative_path,
                    visible_row.id,
                    visible_row.relative_path,
                    Some(normalized_relative_path.clone()),
                    visible_row.selected_thumbnail_image_id,
                    visible_row.image_count,
                    visible_row.child_folder_count,
                )?);
                if folder_batch.len() >= FOLDER_VIEW_BATCH_SIZE {
                    on_batch(std::mem::take(&mut folder_batch), Vec::new())?;
                }
            }
            Ok(())
        })?;
        if !folder_batch.is_empty() {
            on_batch(std::mem::take(&mut folder_batch), Vec::new())?;
        }

        let mut image_batch = Vec::with_capacity(FOLDER_VIEW_BATCH_SIZE);
        self.for_each_image_for_folder(root_id, &normalized_relative_path, |image| {
            image_batch.push(image);
            if image_batch.len() >= FOLDER_VIEW_BATCH_SIZE {
                on_batch(Vec::new(), std::mem::take(&mut image_batch))?;
            }
            Ok(())
        })?;
        if !image_batch.is_empty() {
            on_batch(Vec::new(), image_batch)?;
        }

        Ok(())
    }

    pub fn validate_folder_shallow(&mut self, root_id: &str, relative_path: &str) -> Result<bool> {
        self.validate_folder(root_id, relative_path)
            .map(|validation| validation.changed)
    }

    fn validate_folder(&mut self, _root_id: &str, relative_path: &str) -> Result<FolderValidation> {
        self.ensure_scan_columns()?;

        let normalized_relative_path = normalize_relative_path(relative_path);
        let folder_path = path_from_relative(&self.root_path, &normalized_relative_path);
        if !folder_path.is_dir() {
            let changed = self.delete_folder_subtree(&normalized_relative_path)?;
            return Ok(FolderValidation {
                relative_path: normalized_relative_path,
                changed,
                child_relative_paths: Vec::new(),
                image_count: 0,
                skipped_entries: 1,
            });
        }

        let snapshot = read_directory_snapshot(&self.root_path, &folder_path)?;
        let validation_started = unix_time_ms(SystemTime::now());
        let parent_path = parent_relative_path(&normalized_relative_path);
        let existing_folder_id = self.folder_id_optional(&normalized_relative_path)?;
        let existing_images = if let Some(folder_id) = existing_folder_id {
            self.direct_image_rows(folder_id)?
        } else {
            Vec::new()
        };
        let existing_children = self.direct_child_relative_paths(&normalized_relative_path)?;
        let scanned_child_paths = snapshot
            .child_folders
            .iter()
            .map(|folder| folder.relative_path.clone())
            .collect::<HashSet<_>>();
        let scanned_image_paths = snapshot
            .images
            .iter()
            .map(|image| image.relative_path.clone())
            .collect::<HashSet<_>>();

        let changed = !same_image_entries(&existing_images, &snapshot.images)
            || existing_children != scanned_child_paths
            || existing_folder_id.is_none();

        let tx = self.connection.transaction()?;
        let folder_id = upsert_folder(
            &tx,
            &normalized_relative_path,
            parent_path.as_deref(),
            Some(&snapshot.content_hash),
            validation_started,
        )?;

        for folder in &snapshot.child_folders {
            let child_parent = parent_relative_path(&folder.relative_path);
            upsert_folder(
                &tx,
                &folder.relative_path,
                child_parent.as_deref(),
                None,
                validation_started,
            )?;
        }

        for child_path in existing_children.difference(&scanned_child_paths) {
            delete_folder_subtree_tx(&tx, child_path)?;
        }

        for image_path in existing_images
            .iter()
            .map(|image| image.relative_path.as_str())
            .filter(|relative_path| !scanned_image_paths.contains(*relative_path))
        {
            tx.execute(
                "DELETE FROM images WHERE relative_path = ?1",
                params![image_path],
            )?;
        }

        for image in &snapshot.images {
            upsert_image(
                &tx,
                folder_id,
                &image.file_name,
                &image.relative_path,
                image.file_size,
                image.modified_unix_ms,
                None,
                None,
                validation_started,
            )?;
        }

        tx.commit()?;

        Ok(FolderValidation {
            relative_path: normalized_relative_path,
            changed,
            child_relative_paths: snapshot
                .child_folders
                .into_iter()
                .map(|folder| folder.relative_path)
                .collect(),
            image_count: snapshot.images.len() as u32,
            skipped_entries: snapshot.skipped_entries,
        })
    }

    pub fn image_path(&self, image_id: i64) -> Result<(PathBuf, i64)> {
        let (relative_path, modified_unix_ms): (String, i64) = self
            .connection
            .query_row(
                "SELECT relative_path, modified_unix_ms FROM images WHERE id = ?1",
                params![image_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .with_context(|| format!("image not found: {image_id}"))?;

        Ok((
            path_from_relative(&self.root_path, &relative_path),
            modified_unix_ms,
        ))
    }

    pub fn splat_thumbnail(&self, image_id: i64) -> Result<Option<StoredSplatThumbnail>> {
        self.asset_thumbnail(image_id)
    }

    pub fn asset_thumbnail(&self, image_id: i64) -> Result<Option<StoredSplatThumbnail>> {
        let thumbnail = self
            .connection
            .query_row(
                "
                SELECT images.modified_unix_ms,
                       splat_thumbnails.source_modified_unix_ms,
                       splat_thumbnails.mime_type,
                       splat_thumbnails.data,
                       splat_thumbnails.camera_json
                FROM images
                LEFT JOIN splat_thumbnails ON splat_thumbnails.image_id = images.id
                WHERE images.id = ?1
                ",
                params![image_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<Vec<u8>>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )
            .optional()
            .with_context(|| format!("could not read asset thumbnail for image {image_id}"))?;

        let Some((
            image_modified_unix_ms,
            thumbnail_modified_unix_ms,
            mime_type,
            data,
            camera_json,
        )) = thumbnail
        else {
            bail!("image not found: {image_id}");
        };

        let Some(thumbnail_modified_unix_ms) = thumbnail_modified_unix_ms else {
            return Ok(None);
        };

        if thumbnail_modified_unix_ms != image_modified_unix_ms {
            self.connection.execute(
                "DELETE FROM splat_thumbnails WHERE image_id = ?1",
                params![image_id],
            )?;
            return Ok(None);
        }

        match (mime_type, data) {
            (Some(mime_type), Some(data)) => Ok(Some(StoredSplatThumbnail {
                mime_type,
                data,
                camera_json,
            })),
            _ => Ok(None),
        }
    }

    pub fn save_splat_thumbnail(
        &self,
        image_id: i64,
        mime_type: &str,
        data: &[u8],
        camera_json: Option<&str>,
    ) -> Result<()> {
        self.save_asset_thumbnail(image_id, mime_type, data, camera_json)
    }

    pub fn save_asset_thumbnail(
        &self,
        image_id: i64,
        mime_type: &str,
        data: &[u8],
        camera_json: Option<&str>,
    ) -> Result<()> {
        let modified_unix_ms = self
            .connection
            .query_row(
                "SELECT modified_unix_ms FROM images WHERE id = ?1",
                params![image_id],
                |row| row.get::<_, i64>(0),
            )
            .with_context(|| format!("image not found: {image_id}"))?;

        if !matches!(mime_type, "image/jpeg" | "image/png") {
            bail!("unsupported asset thumbnail type: {mime_type}");
        }

        self.connection.execute(
            "
            INSERT INTO splat_thumbnails(
                image_id,
                source_modified_unix_ms,
                mime_type,
                data,
                camera_json,
                captured_at_unix_ms
            )
            VALUES(?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(image_id) DO UPDATE SET
                source_modified_unix_ms = excluded.source_modified_unix_ms,
                mime_type = excluded.mime_type,
                data = excluded.data,
                camera_json = excluded.camera_json,
                captured_at_unix_ms = excluded.captured_at_unix_ms
            ",
            params![
                image_id,
                modified_unix_ms,
                mime_type,
                data,
                camera_json,
                unix_time_ms(SystemTime::now())
            ],
        )?;

        Ok(())
    }

    pub fn folder_path(&self, relative_path: &str) -> Result<PathBuf> {
        let normalized_relative_path = normalize_relative_path(relative_path);
        self.folder_id(&normalized_relative_path)?;
        Ok(path_from_relative(
            &self.root_path,
            &normalized_relative_path,
        ))
    }

    pub fn recursive_images_for_folder(
        &self,
        root_id: &str,
        folder_relative_path: &str,
    ) -> Result<Vec<ImageSummary>> {
        let normalized_relative_path = normalize_relative_path(folder_relative_path);
        let (lower_bound, upper_bound) = subtree_image_bounds(&normalized_relative_path);
        let mut statement = self.connection.prepare(
            "
            SELECT id, folder_id, file_name, relative_path, width, height, file_size, modified_unix_ms
            FROM images
            WHERE relative_path >= ?1 AND relative_path < ?2
            ORDER BY relative_path COLLATE NOCASE
            ",
        )?;

        let images = statement
            .query_map(params![lower_bound, upper_bound], |row| {
                image_summary_from_row(root_id, row)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(images)
    }

    pub fn refresh_image_metadata(&self, image_id: i64) -> Result<()> {
        let (path, _) = self.image_path(image_id)?;
        let metadata = fs::metadata(&path)
            .with_context(|| format!("could not read metadata for {}", path.display()))?;
        let modified_unix_ms = metadata
            .modified()
            .map(unix_time_ms)
            .unwrap_or_else(|_| unix_time_ms(SystemTime::now()));

        let changed = self.connection.execute(
            "
            UPDATE images
            SET file_size = ?1, modified_unix_ms = ?2
            WHERE id = ?3
            ",
            params![
                metadata.len().min(i64::MAX as u64) as i64,
                modified_unix_ms,
                image_id
            ],
        )?;
        if changed == 0 {
            bail!("image not found: {image_id}");
        }

        Ok(())
    }

    pub fn delete_image(&self, image_id: i64) -> Result<()> {
        let changed = self
            .connection
            .execute("DELETE FROM images WHERE id = ?1", params![image_id])?;
        if changed == 0 {
            bail!("image not found: {image_id}");
        }

        Ok(())
    }

    pub fn delete_folder(&mut self, relative_path: &str) -> Result<()> {
        let normalized_relative_path = normalize_relative_path(relative_path);
        if normalized_relative_path.is_empty() {
            bail!("root folder cannot be moved to the recycle bin");
        }

        self.folder_id(&normalized_relative_path)?;
        if !self.delete_folder_subtree(&normalized_relative_path)? {
            bail!("folder not found: {normalized_relative_path}");
        }

        Ok(())
    }

    pub fn set_folder_thumbnail(&self, folder_id: i64, image_id: i64) -> Result<()> {
        let image_exists = self
            .connection
            .query_row(
                "SELECT 1 FROM images WHERE id = ?1",
                params![image_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !image_exists {
            bail!("image not found: {image_id}");
        }

        let changed = self.connection.execute(
            "UPDATE folders SET selected_thumbnail_image_id = ?1 WHERE id = ?2",
            params![image_id, folder_id],
        )?;
        if changed == 0 {
            bail!("folder not found: {folder_id}");
        }

        Ok(())
    }

    pub fn set_folder_thumbnail_by_path(&self, relative_path: &str, image_id: i64) -> Result<()> {
        let normalized_relative_path = normalize_relative_path(relative_path);
        let folder_id = self.folder_id(&normalized_relative_path)?;
        self.set_folder_thumbnail(folder_id, image_id)
    }

    pub fn folder_metadata(&self, root_id: &str, folder_id: i64) -> Result<FolderMetadata> {
        let relative_path = self.folder_relative_path(folder_id)?;
        let rating = self.folder_rating(folder_id)?;
        let inherited_rating = self.inherited_folder_rating(&relative_path)?;
        let people = self.folder_people(folder_id)?;
        let inherited_people = self.inherited_folder_people(&relative_path)?;
        let tags = self.folder_keywords(folder_id)?;
        let inherited_tags = self.inherited_folder_keywords(&relative_path)?;

        Ok(FolderMetadata {
            root_id: root_id.to_owned(),
            folder_id,
            relative_path,
            rating,
            inherited_rating,
            people,
            inherited_people,
            tags,
            inherited_tags,
        })
    }

    pub fn image_metadata(&self, root_id: &str, image_id: i64) -> Result<ImageMetadata> {
        self.ensure_image_exists(image_id)?;
        let rating = self.image_rating(image_id)?;
        let people = self.image_people(image_id)?;

        Ok(ImageMetadata {
            root_id: root_id.to_owned(),
            image_id,
            rating,
            people,
        })
    }

    pub fn people(&self) -> Result<Vec<MetadataTag>> {
        let mut statement = self.connection.prepare(
            "
            SELECT id, name
            FROM people
            ORDER BY name COLLATE NOCASE
            ",
        )?;
        let people = statement
            .query_map([], metadata_tag_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(people)
    }

    pub fn keywords(&self) -> Result<Vec<MetadataTag>> {
        let mut statement = self.connection.prepare(
            "
            SELECT id, name
            FROM keywords
            ORDER BY name COLLATE NOCASE
            ",
        )?;
        let keywords = statement
            .query_map([], metadata_tag_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(keywords)
    }

    pub fn rename_person(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        rename_metadata_item(
            &mut self.connection,
            "people",
            "person_id",
            &["folder_people", "image_people"],
            old_name,
            new_name,
        )
    }

    pub fn delete_person(&self, name: &str) -> Result<()> {
        delete_metadata_item(&self.connection, "people", name)
    }

    pub fn rename_keyword(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        rename_metadata_item(
            &mut self.connection,
            "keywords",
            "keyword_id",
            &["folder_keywords", "image_keywords"],
            old_name,
            new_name,
        )
    }

    pub fn delete_keyword(&self, name: &str) -> Result<()> {
        delete_metadata_item(&self.connection, "keywords", name)
    }

    pub fn search_folders(
        &self,
        root_id: &str,
        query: &MetadataSearchQuery,
    ) -> Result<Vec<FolderSummary>> {
        let query = NormalizedSearchQuery::from_query(query);
        let mut folders = Vec::new();

        for row in self.candidate_search_folder_rows(&query)? {
            let summary = self.folder_summary(
                root_id,
                ROOT_RELATIVE_PATH,
                row.id,
                row.relative_path,
                row.parent_relative_path,
                row.selected_thumbnail_image_id,
                row.image_count,
                row.child_folder_count,
            )?;
            if folder_matches_search(&summary, &query) {
                folders.push(summary);
            }
        }

        Ok(folders)
    }

    pub fn person_summaries(&self, root_id: &str) -> Result<Vec<MetadataPersonSummary>> {
        let folder_counts = self.effective_person_final_folder_counts()?;
        self.people()?
            .into_iter()
            .map(|person| {
                let thumbnail_image_id = self.person_thumbnail_image_id(person.id)?;
                let folder_count = folder_counts
                    .get(&normalize_search_name(&person.name))
                    .copied()
                    .unwrap_or(0);
                Ok(MetadataPersonSummary {
                    id: person.id,
                    name: person.name,
                    root_id: Some(root_id.to_owned()),
                    thumbnail_image_id,
                    folder_count,
                })
            })
            .collect()
    }

    pub fn add_folder_person(
        &self,
        root_id: &str,
        folder_id: i64,
        name: &str,
    ) -> Result<FolderMetadata> {
        self.folder_relative_path(folder_id)?;
        let name = normalize_metadata_name(name)?;
        self.connection.execute(
            "
            INSERT INTO people(name)
            VALUES(?1)
            ON CONFLICT(name) DO NOTHING
            ",
            params![&name],
        )?;
        let person_id = self.connection.query_row(
            "SELECT id FROM people WHERE name = ?1 COLLATE NOCASE",
            params![&name],
            |row| row.get::<_, i64>(0),
        )?;
        self.connection.execute(
            "
            INSERT INTO folder_people(folder_id, person_id)
            VALUES(?1, ?2)
            ON CONFLICT(folder_id, person_id) DO NOTHING
            ",
            params![folder_id, person_id],
        )?;

        self.folder_metadata(root_id, folder_id)
    }

    pub fn remove_folder_person(
        &self,
        root_id: &str,
        folder_id: i64,
        person_id: i64,
    ) -> Result<FolderMetadata> {
        self.folder_relative_path(folder_id)?;
        self.connection.execute(
            "DELETE FROM folder_people WHERE folder_id = ?1 AND person_id = ?2",
            params![folder_id, person_id],
        )?;
        self.folder_metadata(root_id, folder_id)
    }

    pub fn add_folder_keyword(
        &self,
        root_id: &str,
        folder_id: i64,
        name: &str,
    ) -> Result<FolderMetadata> {
        self.folder_relative_path(folder_id)?;
        let name = normalize_metadata_name(name)?;
        self.connection.execute(
            "
            INSERT INTO keywords(name)
            VALUES(?1)
            ON CONFLICT(name) DO NOTHING
            ",
            params![&name],
        )?;
        let keyword_id = self.connection.query_row(
            "SELECT id FROM keywords WHERE name = ?1 COLLATE NOCASE",
            params![&name],
            |row| row.get::<_, i64>(0),
        )?;
        self.connection.execute(
            "
            INSERT INTO folder_keywords(folder_id, keyword_id)
            VALUES(?1, ?2)
            ON CONFLICT(folder_id, keyword_id) DO NOTHING
            ",
            params![folder_id, keyword_id],
        )?;

        self.folder_metadata(root_id, folder_id)
    }

    pub fn remove_folder_keyword(
        &self,
        root_id: &str,
        folder_id: i64,
        keyword_id: i64,
    ) -> Result<FolderMetadata> {
        self.folder_relative_path(folder_id)?;
        self.connection.execute(
            "DELETE FROM folder_keywords WHERE folder_id = ?1 AND keyword_id = ?2",
            params![folder_id, keyword_id],
        )?;
        self.folder_metadata(root_id, folder_id)
    }

    pub fn set_folder_rating(
        &self,
        root_id: &str,
        folder_id: i64,
        rating: Option<u8>,
    ) -> Result<FolderMetadata> {
        self.folder_relative_path(folder_id)?;
        if let Some(rating) = rating {
            let rating_id = rating_id_for_value(rating)?;
            self.connection.execute(
                "
                INSERT INTO folder_ratings(folder_id, rating_id)
                VALUES(?1, ?2)
                ON CONFLICT(folder_id) DO UPDATE SET rating_id = excluded.rating_id
                ",
                params![folder_id, rating_id],
            )?;
        } else {
            self.connection.execute(
                "DELETE FROM folder_ratings WHERE folder_id = ?1",
                params![folder_id],
            )?;
        }

        self.folder_metadata(root_id, folder_id)
    }

    pub fn add_image_person(
        &self,
        root_id: &str,
        image_id: i64,
        name: &str,
    ) -> Result<ImageMetadata> {
        self.ensure_image_exists(image_id)?;
        let name = normalize_metadata_name(name)?;
        self.connection.execute(
            "
            INSERT INTO people(name)
            VALUES(?1)
            ON CONFLICT(name) DO NOTHING
            ",
            params![&name],
        )?;
        let person_id = self.connection.query_row(
            "SELECT id FROM people WHERE name = ?1 COLLATE NOCASE",
            params![&name],
            |row| row.get::<_, i64>(0),
        )?;
        self.connection.execute(
            "
            INSERT INTO image_people(image_id, person_id)
            VALUES(?1, ?2)
            ON CONFLICT(image_id, person_id) DO NOTHING
            ",
            params![image_id, person_id],
        )?;

        self.image_metadata(root_id, image_id)
    }

    pub fn remove_image_person(
        &self,
        root_id: &str,
        image_id: i64,
        person_id: i64,
    ) -> Result<ImageMetadata> {
        self.ensure_image_exists(image_id)?;
        self.connection.execute(
            "DELETE FROM image_people WHERE image_id = ?1 AND person_id = ?2",
            params![image_id, person_id],
        )?;
        self.image_metadata(root_id, image_id)
    }

    pub fn set_image_rating(
        &self,
        root_id: &str,
        image_id: i64,
        rating: Option<u8>,
    ) -> Result<ImageMetadata> {
        self.ensure_image_exists(image_id)?;
        if let Some(rating) = rating {
            let rating_id = rating_id_for_value(rating)?;
            self.connection.execute(
                "
                INSERT INTO image_ratings(image_id, rating_id)
                VALUES(?1, ?2)
                ON CONFLICT(image_id) DO UPDATE SET rating_id = excluded.rating_id
                ",
                params![image_id, rating_id],
            )?;
        } else {
            self.connection.execute(
                "DELETE FROM image_ratings WHERE image_id = ?1",
                params![image_id],
            )?;
        }

        self.image_metadata(root_id, image_id)
    }

    fn configure(&self) -> Result<()> {
        self.connection
            .busy_timeout(Duration::from_secs(5))
            .context("could not set sqlite busy timeout")?;
        self.connection
            .pragma_update(None, "foreign_keys", "ON")
            .context("could not enable sqlite foreign keys")?;
        Ok(())
    }

    fn init_schema(&self) -> Result<()> {
        self.connection.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;

            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS folders (
                id INTEGER PRIMARY KEY,
                relative_path TEXT NOT NULL UNIQUE,
                parent_relative_path TEXT,
                selected_thumbnail_image_id INTEGER,
                last_seen_scan_ms INTEGER NOT NULL DEFAULT 0,
                content_hash TEXT,
                validated_at_unix_ms INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(selected_thumbnail_image_id) REFERENCES images(id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_folders_parent
                ON folders(parent_relative_path);

            CREATE INDEX IF NOT EXISTS idx_folders_parent_relative_path_nocase
                ON folders(parent_relative_path, relative_path COLLATE NOCASE);

            CREATE TABLE IF NOT EXISTS images (
                id INTEGER PRIMARY KEY,
                folder_id INTEGER NOT NULL,
                file_name TEXT NOT NULL,
                relative_path TEXT NOT NULL UNIQUE,
                file_size INTEGER NOT NULL,
                modified_unix_ms INTEGER NOT NULL,
                width INTEGER,
                height INTEGER,
                scanned_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(folder_id) REFERENCES folders(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_images_folder
                ON images(folder_id);

            CREATE INDEX IF NOT EXISTS idx_images_folder_file_name_nocase
                ON images(folder_id, file_name COLLATE NOCASE);

            CREATE TABLE IF NOT EXISTS keywords (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE COLLATE NOCASE
            );

            CREATE TABLE IF NOT EXISTS people (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE COLLATE NOCASE
            );

            CREATE TABLE IF NOT EXISTS folder_keywords (
                folder_id INTEGER NOT NULL,
                keyword_id INTEGER NOT NULL,
                PRIMARY KEY(folder_id, keyword_id),
                FOREIGN KEY(folder_id) REFERENCES folders(id) ON DELETE CASCADE,
                FOREIGN KEY(keyword_id) REFERENCES keywords(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS folder_people (
                folder_id INTEGER NOT NULL,
                person_id INTEGER NOT NULL,
                PRIMARY KEY(folder_id, person_id),
                FOREIGN KEY(folder_id) REFERENCES folders(id) ON DELETE CASCADE,
                FOREIGN KEY(person_id) REFERENCES people(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_folder_people_person
                ON folder_people(person_id);

            CREATE INDEX IF NOT EXISTS idx_folder_keywords_keyword
                ON folder_keywords(keyword_id);

            CREATE TABLE IF NOT EXISTS ratings (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS folder_ratings (
                folder_id INTEGER PRIMARY KEY,
                rating_id INTEGER NOT NULL,
                FOREIGN KEY(folder_id) REFERENCES folders(id) ON DELETE CASCADE,
                FOREIGN KEY(rating_id) REFERENCES ratings(id) ON DELETE RESTRICT
            );

            CREATE INDEX IF NOT EXISTS idx_folder_ratings_rating
                ON folder_ratings(rating_id);

            CREATE TABLE IF NOT EXISTS image_keywords (
                image_id INTEGER NOT NULL,
                keyword_id INTEGER NOT NULL,
                PRIMARY KEY(image_id, keyword_id),
                FOREIGN KEY(image_id) REFERENCES images(id) ON DELETE CASCADE,
                FOREIGN KEY(keyword_id) REFERENCES keywords(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS image_people (
                image_id INTEGER NOT NULL,
                person_id INTEGER NOT NULL,
                PRIMARY KEY(image_id, person_id),
                FOREIGN KEY(image_id) REFERENCES images(id) ON DELETE CASCADE,
                FOREIGN KEY(person_id) REFERENCES people(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS image_ratings (
                image_id INTEGER PRIMARY KEY,
                rating_id INTEGER NOT NULL,
                FOREIGN KEY(image_id) REFERENCES images(id) ON DELETE CASCADE,
                FOREIGN KEY(rating_id) REFERENCES ratings(id) ON DELETE RESTRICT
            );

            CREATE INDEX IF NOT EXISTS idx_image_people_person
                ON image_people(person_id);

            CREATE INDEX IF NOT EXISTS idx_image_keywords_keyword
                ON image_keywords(keyword_id);

            CREATE TABLE IF NOT EXISTS splat_thumbnails (
                image_id INTEGER PRIMARY KEY,
                source_modified_unix_ms INTEGER NOT NULL,
                mime_type TEXT NOT NULL,
                data BLOB NOT NULL,
                camera_json TEXT,
                captured_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(image_id) REFERENCES images(id) ON DELETE CASCADE
            );
            ",
        )?;

        self.ensure_scan_columns()?;
        self.ensure_splat_thumbnail_columns()?;
        self.ensure_ratings()?;
        self.migrate_image_metadata_to_folders()?;

        self.connection.execute(
            "
            INSERT INTO meta(key, value)
            VALUES('schema_version', ?1)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            ",
            params![SCHEMA_VERSION],
        )?;
        self.connection.execute(
            "
            INSERT INTO folders(relative_path, parent_relative_path, last_seen_scan_ms)
            VALUES('', NULL, 0)
            ON CONFLICT(relative_path) DO NOTHING
            ",
            [],
        )?;
        Ok(())
    }

    fn migrate_image_metadata_to_folders(&self) -> Result<()> {
        self.connection.execute(
            "
            INSERT INTO folder_people(folder_id, person_id)
            SELECT DISTINCT images.folder_id, image_people.person_id
            FROM image_people
            INNER JOIN images ON images.id = image_people.image_id
            ON CONFLICT(folder_id, person_id) DO NOTHING
            ",
            [],
        )?;

        self.connection.execute(
            "
            INSERT INTO folder_ratings(folder_id, rating_id)
            SELECT image_rating_folders.folder_id, MIN(image_rating_folders.rating_id)
            FROM (
                SELECT images.folder_id, image_ratings.rating_id
                FROM image_ratings
                INNER JOIN images ON images.id = image_ratings.image_id
            ) AS image_rating_folders
            WHERE NOT EXISTS (
                SELECT 1
                FROM folder_ratings
                WHERE folder_ratings.folder_id = image_rating_folders.folder_id
            )
            GROUP BY image_rating_folders.folder_id
            HAVING COUNT(DISTINCT image_rating_folders.rating_id) = 1
            ON CONFLICT(folder_id) DO NOTHING
            ",
            [],
        )?;

        Ok(())
    }

    fn ensure_ratings(&self) -> Result<()> {
        self.connection.execute_batch(
            "
            DELETE FROM folder_ratings
            WHERE rating_id IN (
                SELECT id FROM ratings WHERE name IN ('unhappy', 'neutral', 'happy')
            ) OR rating_id NOT BETWEEN 1 AND 5;

            DELETE FROM image_ratings
            WHERE rating_id IN (
                SELECT id FROM ratings WHERE name IN ('unhappy', 'neutral', 'happy')
            ) OR rating_id NOT BETWEEN 1 AND 5;

            DELETE FROM ratings
            WHERE name IN ('unhappy', 'neutral', 'happy') OR id NOT BETWEEN 1 AND 5;
            ",
        )?;

        for (id, name) in [(1_i64, "1"), (2, "2"), (3, "3"), (4, "4"), (5, "5")] {
            self.connection.execute(
                "
                INSERT INTO ratings(id, name)
                VALUES(?1, ?2)
                ON CONFLICT(id) DO UPDATE SET name = excluded.name
                ",
                params![id, name],
            )?;
        }
        Ok(())
    }

    fn ensure_image_exists(&self, image_id: i64) -> Result<()> {
        let exists = self
            .connection
            .query_row(
                "SELECT 1 FROM images WHERE id = ?1",
                params![image_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !exists {
            bail!("image not found: {image_id}");
        }
        Ok(())
    }

    fn image_rating(&self, image_id: i64) -> Result<Option<u8>> {
        let rating_id = self
            .connection
            .query_row(
                "
                SELECT image_ratings.rating_id
                FROM image_ratings
                WHERE image_ratings.image_id = ?1
                ",
                params![image_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .context("could not read image rating")?;
        rating_id.map(rating_from_id).transpose()
    }

    fn image_people(&self, image_id: i64) -> Result<Vec<MetadataTag>> {
        let mut statement = self.connection.prepare(
            "
            SELECT people.id, people.name
            FROM people
            INNER JOIN image_people ON image_people.person_id = people.id
            WHERE image_people.image_id = ?1
            ORDER BY people.name COLLATE NOCASE
            ",
        )?;
        let people = statement
            .query_map(params![image_id], metadata_tag_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(people)
    }

    fn folder_relative_path(&self, folder_id: i64) -> Result<String> {
        self.connection
            .query_row(
                "SELECT relative_path FROM folders WHERE id = ?1",
                params![folder_id],
                |row| row.get(0),
            )
            .with_context(|| format!("folder not found: {folder_id}"))
    }

    fn folder_rating(&self, folder_id: i64) -> Result<Option<u8>> {
        let rating_id = self
            .connection
            .query_row(
                "
                SELECT folder_ratings.rating_id
                FROM folder_ratings
                WHERE folder_ratings.folder_id = ?1
                ",
                params![folder_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .context("could not read folder rating")?;
        rating_id.map(rating_from_id).transpose()
    }

    fn inherited_folder_rating(&self, relative_path: &str) -> Result<Option<u8>> {
        for ancestor in nearest_ancestor_paths(relative_path) {
            let folder_id = self.folder_id(&ancestor)?;
            if let Some(rating) = self.folder_rating(folder_id)? {
                return Ok(Some(rating));
            }
        }

        Ok(None)
    }

    fn folder_people(&self, folder_id: i64) -> Result<Vec<MetadataTag>> {
        let mut statement = self.connection.prepare(
            "
            SELECT people.id, people.name
            FROM people
            INNER JOIN folder_people ON folder_people.person_id = people.id
            WHERE folder_people.folder_id = ?1
            ORDER BY people.name COLLATE NOCASE
            ",
        )?;
        let people = statement
            .query_map(params![folder_id], metadata_tag_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(people)
    }

    fn inherited_folder_people(&self, relative_path: &str) -> Result<Vec<MetadataTag>> {
        let mut people = Vec::new();
        for ancestor in ancestor_paths(relative_path) {
            let folder_id = self.folder_id(&ancestor)?;
            people.extend(self.folder_people(folder_id)?);
        }
        people.sort_by(|left, right| {
            left.name
                .to_lowercase()
                .cmp(&right.name.to_lowercase())
                .then_with(|| left.id.cmp(&right.id))
        });
        people.dedup_by(|left, right| left.id == right.id);
        Ok(people)
    }

    fn folder_keywords(&self, folder_id: i64) -> Result<Vec<MetadataTag>> {
        let mut statement = self.connection.prepare(
            "
            SELECT keywords.id, keywords.name
            FROM keywords
            INNER JOIN folder_keywords ON folder_keywords.keyword_id = keywords.id
            WHERE folder_keywords.folder_id = ?1
            ORDER BY keywords.name COLLATE NOCASE
            ",
        )?;
        let keywords = statement
            .query_map(params![folder_id], metadata_tag_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(keywords)
    }

    fn inherited_folder_keywords(&self, relative_path: &str) -> Result<Vec<MetadataTag>> {
        let mut keywords = Vec::new();
        for ancestor in ancestor_paths(relative_path) {
            let folder_id = self.folder_id(&ancestor)?;
            keywords.extend(self.folder_keywords(folder_id)?);
        }
        keywords.sort_by(|left, right| {
            left.name
                .to_lowercase()
                .cmp(&right.name.to_lowercase())
                .then_with(|| left.id.cmp(&right.id))
        });
        keywords.dedup_by(|left, right| left.id == right.id);
        Ok(keywords)
    }

    fn ensure_column(&self, table: &str, column: &str, definition: &str) -> Result<()> {
        let columns = {
            let mut statement = self
                .connection
                .prepare(&format!("PRAGMA table_info({table})"))
                .with_context(|| format!("could not inspect table {table}"))?;
            statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .with_context(|| format!("could not read columns for table {table}"))?
        };

        if columns.iter().any(|existing| existing == column) {
            return Ok(());
        }

        self.connection
            .execute(
                &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
                [],
            )
            .with_context(|| format!("could not add column {table}.{column}"))?;
        Ok(())
    }

    fn ensure_scan_columns(&self) -> Result<()> {
        self.ensure_column("folders", "content_hash", "TEXT")?;
        self.ensure_column(
            "folders",
            "validated_at_unix_ms",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        Ok(())
    }

    fn ensure_splat_thumbnail_columns(&self) -> Result<()> {
        self.ensure_column("splat_thumbnails", "camera_json", "TEXT")?;
        Ok(())
    }

    fn meta_value(&self, key: &str) -> Result<Option<String>> {
        self.connection
            .query_row(
                "SELECT value FROM meta WHERE key = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()
            .context("could not read database metadata")
    }

    fn set_meta_value(&self, key: &str, value: &str) -> Result<()> {
        self.connection.execute(
            "
            INSERT INTO meta(key, value)
            VALUES(?1, ?2)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            ",
            params![key, value],
        )?;
        Ok(())
    }

    fn child_folders(
        &self,
        root_id: &str,
        parent_relative_path: &str,
    ) -> Result<Vec<FolderSummary>> {
        self.visible_child_folder_rows(parent_relative_path, 0)?
            .into_iter()
            .map(|row| {
                self.folder_summary(
                    root_id,
                    parent_relative_path,
                    row.id,
                    row.relative_path,
                    Some(parent_relative_path.to_owned()),
                    row.selected_thumbnail_image_id,
                    row.image_count,
                    row.child_folder_count,
                )
            })
            .collect()
    }

    fn direct_child_folder_rows(&self, parent_relative_path: &str) -> Result<Vec<FolderRow>> {
        let mut folder_rows = Vec::new();
        self.for_each_direct_child_folder_row(parent_relative_path, |row| {
            folder_rows.push(row);
            Ok(())
        })?;
        Ok(folder_rows)
    }

    fn candidate_search_folder_rows(
        &self,
        query: &NormalizedSearchQuery,
    ) -> Result<Vec<FolderRow>> {
        if let Some(person) = query.person.as_deref() {
            return self.search_folder_rows_for_person(person);
        }

        if !query.include_tags.is_empty() {
            return self
                .search_folder_rows_for_include_tags(&query.include_tags, &query.include_combine);
        }

        if let Some(minimum_rating) = query.minimum_rating {
            return self.search_folder_rows_for_minimum_rating(minimum_rating);
        }

        self.search_folder_rows()
    }

    fn search_folder_rows(&self) -> Result<Vec<FolderRow>> {
        let mut statement = self.connection.prepare(
            "
            SELECT
                id,
                relative_path,
                parent_relative_path,
                selected_thumbnail_image_id,
                content_hash IS NOT NULL AND validated_at_unix_ms > 0 AS validated,
                (SELECT COUNT(*) FROM images WHERE folder_id = folders.id) AS image_count,
                (SELECT COUNT(*) FROM folders AS child
                    WHERE child.parent_relative_path = folders.relative_path) AS child_folder_count
            FROM folders
            ORDER BY relative_path COLLATE NOCASE
            ",
        )?;

        let rows = statement
            .query_map([], |row| {
                Ok(FolderRow {
                    id: row.get::<_, i64>(0)?,
                    relative_path: row.get::<_, String>(1)?,
                    parent_relative_path: row.get::<_, Option<String>>(2)?,
                    selected_thumbnail_image_id: row.get::<_, Option<i64>>(3)?,
                    validated: row.get::<_, i64>(4)? != 0,
                    image_count: row.get::<_, i64>(5)?.max(0) as u32,
                    child_folder_count: row.get::<_, i64>(6)?.max(0) as u32,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    fn search_folder_rows_for_person(&self, person_name: &str) -> Result<Vec<FolderRow>> {
        let assigned_paths = self.person_assigned_folder_paths(person_name)?;
        self.search_folder_rows_for_assigned_paths(assigned_paths)
    }

    fn search_folder_rows_for_include_tags(
        &self,
        tag_names: &[String],
        combine: &MetadataCombineMode,
    ) -> Result<Vec<FolderRow>> {
        let mut assigned_paths = Vec::new();
        let tag_names = match combine {
            MetadataCombineMode::And => tag_names.iter().take(1).collect::<Vec<_>>(),
            MetadataCombineMode::Or => tag_names.iter().collect::<Vec<_>>(),
        };

        for tag_name in tag_names {
            assigned_paths.extend(self.keyword_assigned_folder_paths(tag_name)?);
        }

        self.search_folder_rows_for_assigned_paths(assigned_paths)
    }

    fn search_folder_rows_for_minimum_rating(&self, minimum_rating: u8) -> Result<Vec<FolderRow>> {
        let assigned_paths = self.rating_assigned_folder_paths(minimum_rating)?;
        self.search_folder_rows_for_assigned_paths(assigned_paths)
    }

    fn search_folder_rows_for_assigned_paths(
        &self,
        assigned_paths: Vec<String>,
    ) -> Result<Vec<FolderRow>> {
        let mut rows_by_id = HashMap::new();
        for relative_path in assigned_paths {
            for row in self.subtree_folder_rows(&relative_path)? {
                rows_by_id.entry(row.id).or_insert(row);
            }
        }

        let mut rows = rows_by_id.into_values().collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.relative_path
                .to_lowercase()
                .cmp(&right.relative_path.to_lowercase())
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(rows)
    }

    fn keyword_assigned_folder_paths(&self, keyword_name: &str) -> Result<Vec<String>> {
        let mut statement = self.connection.prepare(
            "
            SELECT folders.relative_path
            FROM folders
            INNER JOIN folder_keywords ON folder_keywords.folder_id = folders.id
            INNER JOIN keywords ON keywords.id = folder_keywords.keyword_id
            WHERE keywords.name = ?1 COLLATE NOCASE
            ORDER BY folders.relative_path COLLATE NOCASE
            ",
        )?;
        let paths = statement
            .query_map(params![keyword_name], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(paths)
    }

    fn rating_assigned_folder_paths(&self, minimum_rating: u8) -> Result<Vec<String>> {
        let minimum_rating_id = rating_id_for_value(minimum_rating)?;
        let mut statement = self.connection.prepare(
            "
            SELECT folders.relative_path
            FROM folders
            INNER JOIN folder_ratings ON folder_ratings.folder_id = folders.id
            WHERE folder_ratings.rating_id >= ?1
            ORDER BY folders.relative_path COLLATE NOCASE
            ",
        )?;
        let paths = statement
            .query_map(params![minimum_rating_id], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(paths)
    }

    fn person_assigned_folder_paths(&self, person_name: &str) -> Result<Vec<String>> {
        let mut statement = self.connection.prepare(
            "
            SELECT folders.relative_path
            FROM folders
            INNER JOIN folder_people ON folder_people.folder_id = folders.id
            INNER JOIN people ON people.id = folder_people.person_id
            WHERE people.name = ?1 COLLATE NOCASE
            ORDER BY folders.relative_path COLLATE NOCASE
            ",
        )?;
        let paths = statement
            .query_map(params![person_name], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(paths)
    }

    fn subtree_folder_rows(&self, relative_path: &str) -> Result<Vec<FolderRow>> {
        let normalized_relative_path = normalize_relative_path(relative_path);
        if normalized_relative_path.is_empty() {
            return self.search_folder_rows();
        }

        let lower_bound = format!("{normalized_relative_path}/");
        let upper_bound = format!("{lower_bound}\u{10ffff}");
        let mut statement = self.connection.prepare(
            "
            SELECT
                id,
                relative_path,
                parent_relative_path,
                selected_thumbnail_image_id,
                content_hash IS NOT NULL AND validated_at_unix_ms > 0 AS validated,
                (SELECT COUNT(*) FROM images WHERE folder_id = folders.id) AS image_count,
                (SELECT COUNT(*) FROM folders AS child
                    WHERE child.parent_relative_path = folders.relative_path) AS child_folder_count
            FROM folders
            WHERE relative_path = ?1
                OR (relative_path >= ?2 AND relative_path < ?3)
            ORDER BY relative_path COLLATE NOCASE
            ",
        )?;

        let rows = statement
            .query_map(
                params![normalized_relative_path, lower_bound, upper_bound],
                |row| {
                    Ok(FolderRow {
                        id: row.get::<_, i64>(0)?,
                        relative_path: row.get::<_, String>(1)?,
                        parent_relative_path: row.get::<_, Option<String>>(2)?,
                        selected_thumbnail_image_id: row.get::<_, Option<i64>>(3)?,
                        validated: row.get::<_, i64>(4)? != 0,
                        image_count: row.get::<_, i64>(5)?.max(0) as u32,
                        child_folder_count: row.get::<_, i64>(6)?.max(0) as u32,
                    })
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    fn final_folder_ids_in_subtree(&self, relative_path: &str) -> Result<Vec<i64>> {
        let normalized_relative_path = normalize_relative_path(relative_path);
        let (where_clause, params): (&str, Vec<String>) = if normalized_relative_path.is_empty() {
            ("", Vec::new())
        } else {
            let lower_bound = format!("{normalized_relative_path}/");
            let upper_bound = format!("{lower_bound}\u{10ffff}");
            (
                "AND (folders.relative_path = ?1 OR (folders.relative_path >= ?2 AND folders.relative_path < ?3))",
                vec![normalized_relative_path, lower_bound, upper_bound],
            )
        };
        let sql = format!(
            "
            SELECT folders.id
            FROM folders
            WHERE EXISTS(
                SELECT 1
                FROM images
                WHERE images.folder_id = folders.id
            )
            {where_clause}
            "
        );
        let mut statement = self.connection.prepare(&sql)?;
        let ids = match params.as_slice() {
            [] => statement
                .query_map([], |row| row.get::<_, i64>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?,
            [relative_path, lower_bound, upper_bound] => statement
                .query_map(params![relative_path, lower_bound, upper_bound], |row| {
                    row.get::<_, i64>(0)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?,
            _ => Vec::new(),
        };
        Ok(ids)
    }

    fn person_thumbnail_image_id(&self, person_id: i64) -> Result<Option<i64>> {
        let thumbnail_folder = self
            .connection
            .query_row(
                "
                SELECT folders.id, folders.relative_path, folders.selected_thumbnail_image_id
                FROM folders
                INNER JOIN folder_people ON folder_people.folder_id = folders.id
                WHERE folder_people.person_id = ?1
                ORDER BY folders.relative_path COLLATE NOCASE
                LIMIT 1
                ",
                params![person_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                    ))
                },
            )
            .optional()
            .context("could not read person thumbnail folder")?;

        Ok(thumbnail_folder
            .map(|(folder_id, relative_path, selected_thumbnail_image_id)| {
                self.thumbnail_image_id(folder_id, &relative_path, selected_thumbnail_image_id)
            })
            .transpose()?
            .flatten())
    }

    fn effective_person_final_folder_counts(&self) -> Result<HashMap<String, u32>> {
        let mut statement = self.connection.prepare(
            "
            SELECT people.name, folders.relative_path
            FROM people
            INNER JOIN folder_people ON folder_people.person_id = people.id
            INNER JOIN folders ON folders.id = folder_people.folder_id
            ORDER BY people.name COLLATE NOCASE, folders.relative_path COLLATE NOCASE
            ",
        )?;
        let assignments = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let mut folder_ids_by_person = HashMap::<String, HashSet<i64>>::new();
        for (person_name, relative_path) in assignments {
            let person_key = normalize_search_name(&person_name);
            if person_key.is_empty() {
                continue;
            }

            let folder_ids = self.final_folder_ids_in_subtree(&relative_path)?;
            folder_ids_by_person
                .entry(person_key)
                .or_default()
                .extend(folder_ids);
        }

        Ok(folder_ids_by_person
            .into_iter()
            .map(|(person, folders)| (person, folders.len().min(u32::MAX as usize) as u32))
            .collect())
    }

    fn for_each_direct_child_folder_row<F>(
        &self,
        parent_relative_path: &str,
        mut on_row: F,
    ) -> Result<()>
    where
        F: FnMut(FolderRow) -> Result<()>,
    {
        let mut statement = self.connection.prepare(
            "
            SELECT
                id,
                relative_path,
                parent_relative_path,
                selected_thumbnail_image_id,
                content_hash IS NOT NULL AND validated_at_unix_ms > 0 AS validated,
                (SELECT COUNT(*) FROM images WHERE folder_id = folders.id) AS image_count,
                (SELECT COUNT(*) FROM folders AS child
                    WHERE child.parent_relative_path = folders.relative_path) AS child_folder_count
            FROM folders
            WHERE parent_relative_path = ?1
            ORDER BY relative_path COLLATE NOCASE
            ",
        )?;

        let mut rows = statement.query(params![parent_relative_path])?;
        while let Some(row) = rows.next()? {
            on_row(FolderRow {
                id: row.get::<_, i64>(0)?,
                relative_path: row.get::<_, String>(1)?,
                parent_relative_path: row.get::<_, Option<String>>(2)?,
                selected_thumbnail_image_id: row.get::<_, Option<i64>>(3)?,
                validated: row.get::<_, i64>(4)? != 0,
                image_count: row.get::<_, i64>(5)?.max(0) as u32,
                child_folder_count: row.get::<_, i64>(6)?.max(0) as u32,
            })?;
        }

        Ok(())
    }

    fn visible_child_folder_rows(
        &self,
        parent_relative_path: &str,
        depth: u8,
    ) -> Result<Vec<FolderRow>> {
        if depth > 64 {
            return Ok(Vec::new());
        }

        let mut visible_rows = Vec::new();
        for row in self.direct_child_folder_rows(parent_relative_path)? {
            visible_rows.extend(self.visible_folder_rows_from(row, depth + 1)?);
        }
        Ok(visible_rows)
    }

    fn visible_folder_rows_from(&self, row: FolderRow, depth: u8) -> Result<Vec<FolderRow>> {
        if depth > 64 {
            return Ok(Vec::new());
        }

        if !row.validated || row.image_count > 0 {
            return Ok(vec![row]);
        }

        let visible_children = self.visible_child_folder_rows(&row.relative_path, depth + 1)?;
        if visible_children.len() == 1 {
            Ok(visible_children)
        } else if visible_children.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(vec![row])
        }
    }

    fn images_for_folder(
        &self,
        root_id: &str,
        folder_relative_path: &str,
    ) -> Result<Vec<ImageSummary>> {
        let mut images = Vec::new();
        self.for_each_image_for_folder(root_id, folder_relative_path, |image| {
            images.push(image);
            Ok(())
        })?;

        Ok(images)
    }

    fn for_each_image_for_folder<F>(
        &self,
        root_id: &str,
        folder_relative_path: &str,
        mut on_image: F,
    ) -> Result<()>
    where
        F: FnMut(ImageSummary) -> Result<()>,
    {
        let folder_id: i64 = self.connection.query_row(
            "SELECT id FROM folders WHERE relative_path = ?1",
            params![folder_relative_path],
            |row| row.get(0),
        )?;

        let mut statement = self.connection.prepare(
            "
            SELECT id, folder_id, file_name, relative_path, width, height, file_size, modified_unix_ms
            FROM images
            WHERE folder_id = ?1
            ORDER BY file_name COLLATE NOCASE
            ",
        )?;

        let mut rows = statement.query(params![folder_id])?;
        while let Some(row) = rows.next()? {
            on_image(image_summary_from_row(root_id, row)?)?;
        }

        Ok(())
    }

    fn folder_summary(
        &self,
        root_id: &str,
        display_parent_relative_path: &str,
        id: i64,
        relative_path: String,
        parent_relative_path: Option<String>,
        selected_thumbnail_image_id: Option<i64>,
        image_count: u32,
        child_folder_count: u32,
    ) -> Result<FolderSummary> {
        let direct_keywords = self.keyword_names(id)?;
        let direct_people = self.person_names(id)?;
        let inherited_keywords = self.inherited_keyword_names(&relative_path)?;
        let inherited_people = self.inherited_person_names(&relative_path)?;
        let direct_rating = self.folder_rating(id)?;
        let inherited_rating = self.inherited_folder_rating(&relative_path)?;
        let thumbnail_image_id =
            self.thumbnail_image_id(id, &relative_path, selected_thumbnail_image_id)?;

        Ok(FolderSummary {
            root_id: root_id.to_owned(),
            id,
            relative_path: relative_path.clone(),
            name: display_name_for_visible_child(display_parent_relative_path, &relative_path),
            parent_relative_path,
            thumbnail_image_id,
            direct_keywords,
            inherited_keywords,
            direct_people,
            inherited_people,
            direct_rating,
            inherited_rating,
            image_count,
            child_folder_count,
        })
    }

    fn thumbnail_image_id(
        &self,
        folder_id: i64,
        relative_path: &str,
        selected_thumbnail_image_id: Option<i64>,
    ) -> Result<Option<i64>> {
        if let Some(image_id) = selected_thumbnail_image_id {
            let exists = self
                .connection
                .query_row(
                    "SELECT id FROM images WHERE id = ?1",
                    params![image_id],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?;
            if exists.is_some() {
                return Ok(Some(image_id));
            }
        }

        if let Some(image_id) = self.first_direct_image_id(folder_id)? {
            return Ok(Some(image_id));
        }

        self.first_subtree_image_id(relative_path)
    }

    fn first_direct_image_id(&self, folder_id: i64) -> Result<Option<i64>> {
        self.connection
            .query_row(
                "SELECT id FROM images WHERE folder_id = ?1 ORDER BY file_name COLLATE NOCASE LIMIT 1",
                params![folder_id],
                |row| row.get(0),
            )
            .optional()
            .context("could not read direct folder thumbnail")
    }

    fn first_subtree_image_id(&self, relative_path: &str) -> Result<Option<i64>> {
        let (lower_bound, upper_bound) = subtree_image_bounds(relative_path);
        self.connection
            .query_row(
                "
                SELECT id
                FROM images
                WHERE relative_path >= ?1 AND relative_path < ?2
                ORDER BY relative_path
                LIMIT 1
                ",
                params![lower_bound, upper_bound],
                |row| row.get(0),
            )
            .optional()
            .context("could not read subtree thumbnail")
    }

    fn keyword_names(&self, folder_id: i64) -> Result<Vec<String>> {
        names_for_folder(
            &self.connection,
            "
            SELECT keywords.name
            FROM keywords
            INNER JOIN folder_keywords ON folder_keywords.keyword_id = keywords.id
            WHERE folder_keywords.folder_id = ?1
            ORDER BY keywords.name COLLATE NOCASE
            ",
            folder_id,
        )
    }

    fn person_names(&self, folder_id: i64) -> Result<Vec<String>> {
        names_for_folder(
            &self.connection,
            "
            SELECT people.name
            FROM people
            INNER JOIN folder_people ON folder_people.person_id = people.id
            WHERE folder_people.folder_id = ?1
            ORDER BY people.name COLLATE NOCASE
            ",
            folder_id,
        )
    }

    fn inherited_keyword_names(&self, relative_path: &str) -> Result<Vec<String>> {
        let mut names = Vec::new();
        for ancestor in ancestor_paths(relative_path) {
            let folder_id = self.folder_id(&ancestor)?;
            names.extend(self.keyword_names(folder_id)?);
        }
        names.sort_by_key(|name| name.to_lowercase());
        names.dedup_by(|a, b| a.eq_ignore_ascii_case(b));
        Ok(names)
    }

    fn inherited_person_names(&self, relative_path: &str) -> Result<Vec<String>> {
        let mut names = Vec::new();
        for ancestor in ancestor_paths(relative_path) {
            let folder_id = self.folder_id(&ancestor)?;
            names.extend(self.person_names(folder_id)?);
        }
        names.sort_by_key(|name| name.to_lowercase());
        names.dedup_by(|a, b| a.eq_ignore_ascii_case(b));
        Ok(names)
    }

    fn folder_id(&self, relative_path: &str) -> Result<i64> {
        self.connection
            .query_row(
                "SELECT id FROM folders WHERE relative_path = ?1",
                params![relative_path],
                |row| row.get(0),
            )
            .with_context(|| format!("folder not found: {relative_path}"))
    }

    fn folder_id_optional(&self, relative_path: &str) -> Result<Option<i64>> {
        self.connection
            .query_row(
                "SELECT id FROM folders WHERE relative_path = ?1",
                params![relative_path],
                |row| row.get(0),
            )
            .optional()
            .context("could not read folder id")
    }

    fn direct_child_relative_paths(&self, parent_relative_path: &str) -> Result<HashSet<String>> {
        let mut statement = self
            .connection
            .prepare("SELECT relative_path FROM folders WHERE parent_relative_path = ?1")?;
        let rows = statement
            .query_map(params![parent_relative_path], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows.into_iter().collect())
    }

    fn direct_image_rows(&self, folder_id: i64) -> Result<Vec<ExistingImage>> {
        let mut statement = self.connection.prepare(
            "
            SELECT file_name, relative_path, file_size, modified_unix_ms
            FROM images
            WHERE folder_id = ?1
            ",
        )?;
        let rows = statement
            .query_map(params![folder_id], |row| {
                Ok(ExistingImage {
                    file_name: row.get(0)?,
                    relative_path: row.get(1)?,
                    file_size: row.get::<_, i64>(2)?.max(0) as u64,
                    modified_unix_ms: row.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    fn delete_folder_subtree(&mut self, relative_path: &str) -> Result<bool> {
        let tx = self.connection.transaction()?;
        let changed = delete_folder_subtree_tx(&tx, relative_path)?;
        tx.commit()?;
        Ok(changed)
    }

    fn visible_parent_relative_path(&self, relative_path: &str) -> Result<Option<String>> {
        let mut candidate = parent_relative_path(relative_path);
        while let Some(parent) = candidate {
            if parent.is_empty() || !self.is_pass_through_folder(&parent)? {
                return Ok(Some(parent));
            }
            candidate = parent_relative_path(&parent);
        }

        Ok(None)
    }

    fn is_pass_through_folder(&self, relative_path: &str) -> Result<bool> {
        Ok(!self.folder_has_direct_images(relative_path)?
            && self.visible_child_folder_rows(relative_path, 0)?.len() == 1)
    }

    fn folder_has_direct_images(&self, relative_path: &str) -> Result<bool> {
        let count = self.connection.query_row(
            "
            SELECT COUNT(images.id)
            FROM folders
            LEFT JOIN images ON images.folder_id = folders.id
            WHERE folders.relative_path = ?1
            ",
            params![relative_path],
            |row| row.get::<_, i64>(0),
        )?;

        Ok(count > 0)
    }
}

fn upsert_folder(
    tx: &Transaction<'_>,
    relative_path: &str,
    parent_relative_path: Option<&str>,
    content_hash: Option<&str>,
    validation_started: i64,
) -> Result<i64> {
    tx.execute(
        "
        INSERT INTO folders(
            relative_path,
            parent_relative_path,
            last_seen_scan_ms,
            content_hash,
            validated_at_unix_ms
        )
        VALUES(?1, ?2, ?3, ?4, CASE WHEN ?4 IS NULL THEN 0 ELSE ?3 END)
        ON CONFLICT(relative_path) DO UPDATE SET
            parent_relative_path = excluded.parent_relative_path,
            last_seen_scan_ms = excluded.last_seen_scan_ms,
            content_hash = COALESCE(excluded.content_hash, folders.content_hash),
            validated_at_unix_ms = CASE
                WHEN excluded.content_hash IS NULL THEN folders.validated_at_unix_ms
                ELSE excluded.validated_at_unix_ms
            END
        ",
        params![
            relative_path,
            parent_relative_path,
            validation_started,
            content_hash
        ],
    )?;

    tx.query_row(
        "SELECT id FROM folders WHERE relative_path = ?1",
        params![relative_path],
        |row| row.get(0),
    )
    .context("could not read folder id")
}

#[allow(clippy::too_many_arguments)]
fn upsert_image(
    tx: &Transaction<'_>,
    folder_id: i64,
    file_name: &str,
    relative_path: &str,
    file_size: u64,
    modified_unix_ms: i64,
    width: Option<u32>,
    height: Option<u32>,
    scan_started: i64,
) -> Result<()> {
    tx.execute(
        "
        INSERT INTO images(
            folder_id,
            file_name,
            relative_path,
            file_size,
            modified_unix_ms,
            width,
            height,
            scanned_at_unix_ms
        )
        VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        ON CONFLICT(relative_path) DO UPDATE SET
            folder_id = excluded.folder_id,
            file_name = excluded.file_name,
            file_size = excluded.file_size,
            modified_unix_ms = excluded.modified_unix_ms,
            width = excluded.width,
            height = excluded.height,
            scanned_at_unix_ms = excluded.scanned_at_unix_ms
        ",
        params![
            folder_id,
            file_name,
            relative_path,
            file_size.min(i64::MAX as u64) as i64,
            modified_unix_ms,
            width.map(i64::from),
            height.map(i64::from),
            scan_started
        ],
    )?;

    Ok(())
}

fn delete_folder_subtree_tx(tx: &Transaction<'_>, relative_path: &str) -> Result<bool> {
    let changed = if relative_path.is_empty() {
        let images_deleted = tx.execute("DELETE FROM images", [])?;
        let folders_deleted = tx.execute("DELETE FROM folders WHERE relative_path <> ''", [])?;
        images_deleted + folders_deleted
    } else {
        let (lower_bound, upper_bound) = subtree_image_bounds(relative_path);
        let images_deleted = tx.execute(
            "
            DELETE FROM images
            WHERE relative_path = ?1 OR (relative_path >= ?2 AND relative_path < ?3)
            ",
            params![relative_path, lower_bound, upper_bound],
        )?;
        let folders_deleted = tx.execute(
            "
            DELETE FROM folders
            WHERE relative_path = ?1 OR (relative_path >= ?2 AND relative_path < ?3)
            ",
            params![relative_path, lower_bound, upper_bound],
        )?;
        images_deleted + folders_deleted
    };

    Ok(changed > 0)
}

fn same_image_entries(existing: &[ExistingImage], scanned: &[ScannedImage]) -> bool {
    if existing.len() != scanned.len() {
        return false;
    }

    let existing = existing
        .iter()
        .map(|image| {
            (
                image.relative_path.as_str(),
                (
                    image.file_name.as_str(),
                    image.file_size,
                    image.modified_unix_ms,
                ),
            )
        })
        .collect::<HashMap<_, _>>();

    scanned.iter().all(|image| {
        existing.get(image.relative_path.as_str()).is_some_and(
            |(file_name, file_size, modified_unix_ms)| {
                *file_name == image.file_name
                    && *file_size == image.file_size
                    && *modified_unix_ms == image.modified_unix_ms
            },
        )
    })
}

fn read_directory_snapshot(root_path: &Path, folder_path: &Path) -> Result<DirectorySnapshot> {
    let mut child_folders = Vec::new();
    let mut images = Vec::new();
    let mut skipped_entries = 0_u32;

    match fs::read_dir(folder_path) {
        Ok(entries) => {
            for entry in entries {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(_) => {
                        skipped_entries += 1;
                        continue;
                    }
                };

                let path = entry.path();
                let file_name = entry.file_name().to_string_lossy().to_string();
                let file_type = match entry.file_type() {
                    Ok(file_type) => file_type,
                    Err(_) => {
                        skipped_entries += 1;
                        continue;
                    }
                };

                if file_type.is_dir() {
                    if should_skip_scanned_folder(&entry, &file_name) {
                        continue;
                    }
                    let (modified_unix_ms, metadata_ok) = entry_modified_unix_ms(&entry);
                    if !metadata_ok {
                        skipped_entries += 1;
                    }
                    child_folders.push(ScannedFolder {
                        file_name,
                        relative_path: relative_path_for(root_path, &path)?,
                        modified_unix_ms,
                    });
                    continue;
                }

                if !file_type.is_file()
                    || is_picturious_sidecar_thumbnail(&path)
                    || !is_supported_media(&path)
                {
                    continue;
                }

                let (file_size, modified_unix_ms, metadata_ok) = entry_file_fingerprint(&entry);
                if !metadata_ok {
                    skipped_entries += 1;
                }
                images.push(ScannedImage {
                    file_name,
                    relative_path: relative_path_for(root_path, &path)?,
                    file_size,
                    modified_unix_ms,
                });
            }
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("could not read directory {}", folder_path.display()));
        }
    }

    child_folders.sort_by(|left, right| {
        left.relative_path
            .to_lowercase()
            .cmp(&right.relative_path.to_lowercase())
            .then_with(|| left.relative_path.cmp(&right.relative_path))
    });
    images.sort_by(|left, right| {
        left.file_name
            .to_lowercase()
            .cmp(&right.file_name.to_lowercase())
            .then_with(|| left.relative_path.cmp(&right.relative_path))
    });

    let content_hash = directory_content_hash(&child_folders, &images);

    Ok(DirectorySnapshot {
        child_folders,
        images,
        content_hash,
        skipped_entries,
    })
}

fn should_skip_scanned_folder(entry: &fs::DirEntry, file_name: &str) -> bool {
    is_ignored_folder_name(file_name) || entry_has_hidden_or_system_attribute(entry)
}

fn is_ignored_folder_name(file_name: &str) -> bool {
    file_name.eq_ignore_ascii_case(DB_DIR)
        || file_name.starts_with('.')
        || file_name.eq_ignore_ascii_case("$Recycle.Bin")
        || file_name.eq_ignore_ascii_case("System Volume Information")
}

#[cfg(windows)]
fn entry_has_hidden_or_system_attribute(entry: &fs::DirEntry) -> bool {
    entry.metadata().is_ok_and(|metadata| {
        let attributes = metadata.file_attributes();
        attributes & (FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_SYSTEM) != 0
    })
}

#[cfg(not(windows))]
fn entry_has_hidden_or_system_attribute(_entry: &fs::DirEntry) -> bool {
    false
}

fn entry_modified_unix_ms(entry: &fs::DirEntry) -> (i64, bool) {
    match entry.metadata().and_then(|metadata| metadata.modified()) {
        Ok(modified) => (unix_time_ms(modified), true),
        Err(_) => (0, false),
    }
}

fn entry_file_fingerprint(entry: &fs::DirEntry) -> (u64, i64, bool) {
    match entry.metadata() {
        Ok(metadata) => {
            let modified_unix_ms = metadata.modified().map(unix_time_ms).unwrap_or(0);
            (metadata.len(), modified_unix_ms, true)
        }
        Err(_) => (0, 0, false),
    }
}

fn directory_content_hash(child_folders: &[ScannedFolder], images: &[ScannedImage]) -> String {
    let mut hash = FNV_OFFSET_BASIS;
    for folder in child_folders {
        fnv_update_str(&mut hash, "D");
        fnv_update_str(&mut hash, &folder.file_name);
        fnv_update_str(&mut hash, &folder.relative_path);
        fnv_update_i64(&mut hash, folder.modified_unix_ms);
    }
    for image in images {
        fnv_update_str(&mut hash, "I");
        fnv_update_str(&mut hash, &image.file_name);
        fnv_update_str(&mut hash, &image.relative_path);
        fnv_update_u64(&mut hash, image.file_size);
        fnv_update_i64(&mut hash, image.modified_unix_ms);
    }
    format!("{hash:016x}")
}

const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x00000100000001b3;

fn fnv_update_str(hash: &mut u64, value: &str) {
    for byte in value.as_bytes() {
        fnv_update_byte(hash, *byte);
    }
    fnv_update_byte(hash, 0xff);
}

fn fnv_update_u64(hash: &mut u64, value: u64) {
    for byte in value.to_le_bytes() {
        fnv_update_byte(hash, byte);
    }
}

fn fnv_update_i64(hash: &mut u64, value: i64) {
    fnv_update_u64(hash, value as u64);
}

fn fnv_update_byte(hash: &mut u64, byte: u8) {
    *hash ^= u64::from(byte);
    *hash = hash.wrapping_mul(FNV_PRIME);
}

struct NormalizedSearchQuery {
    person: Option<String>,
    include_tags: Vec<String>,
    include_combine: MetadataCombineMode,
    exclude_tags: Vec<String>,
    exclude_combine: MetadataCombineMode,
    minimum_rating: Option<u8>,
}

impl NormalizedSearchQuery {
    fn from_query(query: &MetadataSearchQuery) -> Self {
        Self {
            person: query
                .person
                .as_deref()
                .map(normalize_search_name)
                .filter(|name| !name.is_empty()),
            include_tags: normalized_search_names(&query.include_tags.names),
            include_combine: query.include_tags.combine.clone(),
            exclude_tags: normalized_search_names(&query.exclude_tags.names),
            exclude_combine: query.exclude_tags.combine.clone(),
            minimum_rating: query
                .minimum_rating
                .filter(|rating| (1..=5).contains(rating)),
        }
    }
}

fn folder_matches_search(folder: &FolderSummary, query: &NormalizedSearchQuery) -> bool {
    let people = effective_name_set(&folder.direct_people, &folder.inherited_people);
    if let Some(person) = query.person.as_ref() {
        if !people.contains(person) {
            return false;
        }
    }

    let tags = effective_name_set(&folder.direct_keywords, &folder.inherited_keywords);
    if !query.include_tags.is_empty()
        && !name_filter_matches(&tags, &query.include_tags, &query.include_combine)
    {
        return false;
    }
    if !query.exclude_tags.is_empty()
        && name_filter_matches(&tags, &query.exclude_tags, &query.exclude_combine)
    {
        return false;
    }

    if let Some(minimum_rating) = query.minimum_rating {
        let rating = folder.direct_rating.or(folder.inherited_rating);
        if !rating.is_some_and(|rating| rating >= minimum_rating) {
            return false;
        }
    }

    true
}

fn effective_name_set(direct_names: &[String], inherited_names: &[String]) -> HashSet<String> {
    direct_names
        .iter()
        .chain(inherited_names.iter())
        .map(|name| normalize_search_name(name))
        .filter(|name| !name.is_empty())
        .collect()
}

fn name_filter_matches(
    candidates: &HashSet<String>,
    filter_names: &[String],
    combine: &MetadataCombineMode,
) -> bool {
    if filter_names.is_empty() {
        return false;
    }

    match combine {
        MetadataCombineMode::And => filter_names.iter().all(|name| candidates.contains(name)),
        MetadataCombineMode::Or => filter_names.iter().any(|name| candidates.contains(name)),
    }
}

fn normalized_search_names(names: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    for name in names {
        let name = normalize_search_name(name);
        if !name.is_empty() && !normalized.contains(&name) {
            normalized.push(name);
        }
    }
    normalized
}

fn normalize_search_name(name: &str) -> String {
    name.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn names_for_folder(connection: &Connection, sql: &str, folder_id: i64) -> Result<Vec<String>> {
    let mut statement = connection.prepare(sql)?;
    let names = statement
        .query_map(params![folder_id], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(names)
}

fn metadata_tag_from_row(row: &Row<'_>) -> rusqlite::Result<MetadataTag> {
    Ok(MetadataTag {
        id: row.get(0)?,
        name: row.get(1)?,
    })
}

fn rename_metadata_item(
    connection: &mut Connection,
    metadata_table: &str,
    reference_column: &str,
    reference_tables: &[&str],
    old_name: &str,
    new_name: &str,
) -> Result<()> {
    let old_name = normalize_metadata_name(old_name)?;
    let new_name = normalize_metadata_name(new_name)?;
    let tx = connection.transaction()?;
    let Some(old_id) = metadata_id_by_name(&tx, metadata_table, &old_name)? else {
        tx.commit()?;
        return Ok(());
    };

    if metadata_names_equal(&old_name, &new_name) {
        tx.execute(
            &format!("UPDATE {metadata_table} SET name = ?1 WHERE id = ?2"),
            params![new_name, old_id],
        )?;
        tx.commit()?;
        return Ok(());
    }

    if let Some(new_id) = metadata_id_by_name(&tx, metadata_table, &new_name)? {
        for reference_table in reference_tables {
            merge_metadata_references(&tx, reference_table, reference_column, old_id, new_id)?;
        }
        tx.execute(
            &format!("DELETE FROM {metadata_table} WHERE id = ?1"),
            params![old_id],
        )?;
    } else {
        tx.execute(
            &format!("UPDATE {metadata_table} SET name = ?1 WHERE id = ?2"),
            params![new_name, old_id],
        )?;
    }

    tx.commit()?;
    Ok(())
}

fn delete_metadata_item(connection: &Connection, metadata_table: &str, name: &str) -> Result<()> {
    let name = normalize_metadata_name(name)?;
    connection.execute(
        &format!("DELETE FROM {metadata_table} WHERE name = ?1 COLLATE NOCASE"),
        params![name],
    )?;
    Ok(())
}

fn metadata_id_by_name(
    tx: &Transaction<'_>,
    metadata_table: &str,
    name: &str,
) -> Result<Option<i64>> {
    tx.query_row(
        &format!("SELECT id FROM {metadata_table} WHERE name = ?1 COLLATE NOCASE"),
        params![name],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .context("could not read metadata item")
}

fn merge_metadata_references(
    tx: &Transaction<'_>,
    reference_table: &str,
    reference_column: &str,
    old_id: i64,
    new_id: i64,
) -> Result<()> {
    let owner_column = if reference_table.starts_with("folder_") {
        "folder_id"
    } else {
        "image_id"
    };
    tx.execute(
        &format!(
            "INSERT OR IGNORE INTO {reference_table}({owner_column}, {reference_column})
             SELECT {owner_column}, ?1 FROM {reference_table} WHERE {reference_column} = ?2"
        ),
        params![new_id, old_id],
    )?;
    Ok(())
}

fn normalize_metadata_name(name: &str) -> Result<String> {
    let normalized = name.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        bail!("metadata name is empty");
    }
    if normalized.chars().count() > 160 {
        bail!("metadata name is too long");
    }
    Ok(normalized)
}

fn metadata_names_equal(left: &str, right: &str) -> bool {
    left.to_lowercase() == right.to_lowercase()
}

fn rating_id_for_value(value: u8) -> Result<i64> {
    if (1..=5).contains(&value) {
        Ok(i64::from(value))
    } else {
        bail!("unknown rating: {value}")
    }
}

fn rating_from_id(id: i64) -> Result<u8> {
    let value: u8 = id
        .try_into()
        .with_context(|| format!("unknown rating: {id}"))?;
    if (1..=5).contains(&value) {
        Ok(value)
    } else {
        bail!("unknown rating: {id}")
    }
}

fn image_summary_from_row(root_id: &str, row: &Row<'_>) -> rusqlite::Result<ImageSummary> {
    Ok(ImageSummary {
        root_id: root_id.to_owned(),
        id: row.get(0)?,
        folder_id: row.get(1)?,
        file_name: row.get(2)?,
        relative_path: row.get(3)?,
        width: row
            .get::<_, Option<i64>>(4)?
            .map(|value| value.max(0) as u32),
        height: row
            .get::<_, Option<i64>>(5)?
            .map(|value| value.max(0) as u32),
        file_size: row.get::<_, i64>(6)?.max(0) as u64,
        modified_unix_ms: row.get(7)?,
    })
}

fn relative_path_for(root_path: &Path, path: &Path) -> Result<String> {
    let relative = path
        .strip_prefix(root_path)
        .with_context(|| format!("{} is not below {}", path.display(), root_path.display()))?;

    let parts = relative
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_string_lossy().to_string()),
            _ => None,
        })
        .collect::<Vec<_>>();

    Ok(parts.join("/"))
}

fn path_from_relative(root_path: &Path, relative_path: &str) -> PathBuf {
    relative_path
        .split('/')
        .filter(|part| !part.is_empty())
        .fold(root_path.to_path_buf(), |path, part| path.join(part))
}

fn normalize_relative_path(relative_path: &str) -> String {
    relative_path
        .replace('\\', "/")
        .trim_matches('/')
        .to_owned()
}

fn subtree_image_bounds(relative_path: &str) -> (String, String) {
    let lower_bound = if relative_path.is_empty() {
        String::new()
    } else {
        format!("{relative_path}/")
    };
    let upper_bound = format!("{lower_bound}\u{10ffff}");
    (lower_bound, upper_bound)
}

fn parent_relative_path(relative_path: &str) -> Option<String> {
    if relative_path.is_empty() {
        None
    } else {
        Some(
            relative_path
                .rsplit_once('/')
                .map(|(parent, _)| parent.to_owned())
                .unwrap_or_default(),
        )
    }
}

fn ancestor_paths(relative_path: &str) -> Vec<String> {
    if relative_path.is_empty() {
        return Vec::new();
    }

    let mut ancestors = vec![String::new()];
    let mut current = relative_path.to_owned();
    while let Some((parent, _)) = current.rsplit_once('/') {
        if parent.is_empty() {
            break;
        }
        ancestors.push(parent.to_owned());
        current = parent.to_owned();
    }
    ancestors
}

fn nearest_ancestor_paths(relative_path: &str) -> Vec<String> {
    let mut ancestors = Vec::new();
    let mut current = parent_relative_path(relative_path);
    while let Some(path) = current {
        ancestors.push(path.clone());
        if path.is_empty() {
            break;
        }
        current = parent_relative_path(&path);
    }
    ancestors
}

fn display_name_for_relative_path(relative_path: &str) -> String {
    if relative_path.is_empty() {
        "Root".to_owned()
    } else {
        relative_path
            .rsplit('/')
            .next()
            .filter(|name| !name.is_empty())
            .unwrap_or(relative_path)
            .to_owned()
    }
}

fn display_name_for_visible_child(parent_relative_path: &str, relative_path: &str) -> String {
    let parent = normalize_relative_path(parent_relative_path);
    let relative = normalize_relative_path(relative_path);
    let visible_name = if parent.is_empty() {
        relative.as_str()
    } else {
        relative
            .strip_prefix(&format!("{parent}/"))
            .unwrap_or(relative.as_str())
    };

    if visible_name.is_empty() {
        display_name_for_relative_path(&relative)
    } else {
        visible_name.replace('/', "\\")
    }
}

fn is_supported_media(path: &Path) -> bool {
    is_supported_image(path) || is_supported_splat(path) || is_supported_model(path)
}

fn is_supported_image(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            SUPPORTED_IMAGE_EXTENSIONS
                .iter()
                .any(|supported| extension.eq_ignore_ascii_case(supported))
        })
        .unwrap_or(false)
}

fn is_supported_splat(path: &Path) -> bool {
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
            SUPPORTED_SPLAT_EXTENSIONS
                .iter()
                .any(|supported| extension.eq_ignore_ascii_case(supported))
        })
        .unwrap_or(false)
}

fn is_supported_model(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            SUPPORTED_MODEL_EXTENSIONS
                .iter()
                .any(|supported| extension.eq_ignore_ascii_case(supported))
        })
        .unwrap_or(false)
}

fn is_picturious_sidecar_thumbnail(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.to_ascii_lowercase().ends_with(".picturious-thumb.jpg"))
        .unwrap_or(false)
}

fn unix_time_ms(system_time: SystemTime) -> i64 {
    system_time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

pub fn clean_path_string(path: &Path) -> String {
    path.to_string_lossy()
        .trim_start_matches(r"\\?\")
        .to_owned()
}

pub fn root_display_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| clean_path_string(path))
}

pub fn validate_root_path(path: &str) -> Result<PathBuf> {
    let trimmed = path.trim().trim_matches('"').trim();
    if trimmed.is_empty() {
        bail!("root path is empty");
    }

    let path = PathBuf::from(trimmed);
    let path = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()?.join(path)
    };

    if !path.is_dir() {
        return Err(anyhow!("root path is not a directory: {}", path.display()));
    }

    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::MetadataNameFilter;

    #[test]
    fn image_metadata_people_and_rating_round_trip() -> Result<()> {
        let root = temp_root_path("image_metadata_people_and_rating_round_trip");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root)?;
        fs::write(root.join("portrait.jpg"), b"image bytes")?;

        let mut db = RootDatabase::open(&root)?;
        let root_id = db.root_id()?;
        db.scan(&root_id)?;
        let images = db.images_for_folder(&root_id, "")?;
        let image_id = images
            .first()
            .map(|image| image.id)
            .context("test image was not indexed")?;

        let metadata = db.add_image_person(&root_id, image_id, " Ada   Lovelace ")?;
        assert_eq!(metadata.people.len(), 1);
        assert_eq!(metadata.people[0].name, "Ada Lovelace");

        let metadata = db.set_image_rating(&root_id, image_id, Some(4))?;
        assert_eq!(metadata.rating, Some(4));

        let metadata = db.set_image_rating(&root_id, image_id, None)?;
        assert_eq!(metadata.rating, None);
        assert_eq!(metadata.people.len(), 1);

        let person_id = metadata.people[0].id;
        let metadata = db.remove_image_person(&root_id, image_id, person_id)?;
        assert!(metadata.people.is_empty());

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn scan_indexes_splats_and_ignores_sidecar_thumbnails() -> Result<()> {
        let root = temp_root_path("scan_indexes_splats_and_ignores_sidecar_thumbnails");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root)?;
        fs::write(root.join("scene.spz"), b"splat bytes")?;
        fs::write(
            root.join("scene.spz.picturious-thumb.jpg"),
            b"thumbnail bytes",
        )?;

        let mut db = RootDatabase::open(&root)?;
        let root_id = db.root_id()?;
        db.scan(&root_id)?;

        let images = db.images_for_folder(&root_id, "")?;
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].file_name, "scene.spz");

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn scan_indexes_glb_models() -> Result<()> {
        let root = temp_root_path("scan_indexes_glb_models");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root)?;
        fs::write(root.join("statue.glb"), b"glb bytes")?;

        let mut db = RootDatabase::open(&root)?;
        let root_id = db.root_id()?;
        db.scan(&root_id)?;

        let images = db.images_for_folder(&root_id, "")?;
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].file_name, "statue.glb");

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn scan_ignores_zip_archives() -> Result<()> {
        let root = temp_root_path("scan_ignores_zip_archives");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root)?;
        fs::write(root.join("archive.zip"), b"zip bytes")?;

        let mut db = RootDatabase::open(&root)?;
        let root_id = db.root_id()?;
        db.scan(&root_id)?;

        assert!(db.images_for_folder(&root_id, "")?.is_empty());

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn scan_indexes_heic_images() -> Result<()> {
        let root = temp_root_path("scan_indexes_heic_images");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root)?;
        fs::write(root.join("iphone.heic"), b"heic bytes")?;
        fs::write(root.join("render.heif"), b"heif bytes")?;

        let mut db = RootDatabase::open(&root)?;
        let root_id = db.root_id()?;
        db.scan(&root_id)?;

        let image_names = db
            .images_for_folder(&root_id, "")?
            .into_iter()
            .map(|image| image.file_name)
            .collect::<Vec<_>>();
        assert_eq!(image_names, vec!["iphone.heic", "render.heif"]);

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn scan_ignores_hidden_and_system_folders() -> Result<()> {
        let root = temp_root_path("scan_ignores_hidden_and_system_folders");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join(".hidden"))?;
        fs::create_dir_all(root.join("$Recycle.Bin"))?;
        fs::create_dir_all(root.join("System Volume Information"))?;
        fs::create_dir_all(root.join("Visible"))?;
        fs::write(root.join(".hidden").join("secret.jpg"), b"image bytes")?;
        fs::write(
            root.join("$Recycle.Bin").join("recycled.jpg"),
            b"image bytes",
        )?;
        fs::write(
            root.join("System Volume Information")
                .join("system-volume.jpg"),
            b"image bytes",
        )?;
        fs::write(root.join("Visible").join("photo.jpg"), b"image bytes")?;

        #[cfg(windows)]
        let attributed_folder = {
            use std::process::Command;

            let folder = root.join("HiddenAttribute");
            fs::create_dir_all(&folder)?;
            fs::write(folder.join("hidden-attribute.jpg"), b"image bytes")?;
            let marked = Command::new("attrib")
                .arg("+h")
                .arg("+s")
                .arg(&folder)
                .status()
                .is_ok_and(|status| status.success());
            if marked {
                Some(folder)
            } else {
                fs::remove_dir_all(&folder)?;
                None
            }
        };

        let mut db = RootDatabase::open(&root)?;
        let root_id = db.root_id()?;
        db.scan(&root_id)?;

        let root_view = db.folder_view(&root_id, "Root", "")?;
        let folder_names = root_view
            .folders
            .iter()
            .map(|folder| folder.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(folder_names, vec!["Visible"]);
        assert!(db.folder_id_optional(".hidden")?.is_none());
        assert!(db.folder_id_optional("$Recycle.Bin")?.is_none());
        assert!(
            db.folder_id_optional("System Volume Information")?
                .is_none()
        );
        #[cfg(windows)]
        if attributed_folder.is_some() {
            assert!(db.folder_id_optional("HiddenAttribute")?.is_none());
        }

        #[cfg(windows)]
        if let Some(folder) = attributed_folder {
            use std::process::Command;

            let _ = Command::new("attrib")
                .arg("-h")
                .arg("-s")
                .arg(&folder)
                .status();
        }
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn rescan_descends_into_unchanged_parent_folders() -> Result<()> {
        let root = temp_root_path("rescan_descends_into_unchanged_parent_folders");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("Parent").join("Child"))?;
        fs::write(
            root.join("Parent").join("Child").join("old.jpg"),
            b"image bytes",
        )?;

        let mut db = RootDatabase::open(&root)?;
        let root_id = db.root_id()?;
        db.scan(&root_id)?;

        fs::write(
            root.join("Parent").join("Child").join("new.jpg"),
            b"image bytes",
        )?;
        let root_snapshot = read_directory_snapshot(&root, &root)?;
        db.connection.execute(
            "UPDATE folders SET content_hash = ?1 WHERE relative_path = ''",
            params![root_snapshot.content_hash],
        )?;

        db.rescan_with_progress(&root_id, "", |_| {})?;

        let child_view = db.folder_view(&root_id, "Root", "Parent/Child")?;
        let image_names = child_view
            .images
            .iter()
            .map(|image| image.file_name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(image_names, vec!["new.jpg", "old.jpg"]);

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn splat_thumbnail_round_trips_and_cascades_on_delete() -> Result<()> {
        let root = temp_root_path("splat_thumbnail_round_trips_and_cascades_on_delete");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root)?;
        fs::write(root.join("scene.spz"), b"splat bytes")?;

        let mut db = RootDatabase::open(&root)?;
        let root_id = db.root_id()?;
        db.scan(&root_id)?;
        let image_id = db
            .images_for_folder(&root_id, "")?
            .first()
            .map(|image| image.id)
            .context("splat was not indexed")?;

        db.save_splat_thumbnail(
            image_id,
            "image/jpeg",
            b"jpeg bytes",
            Some(r#"{"version":1,"position":[1,2,3],"focus":[4,5,6]}"#),
        )?;
        let thumbnail = db
            .splat_thumbnail(image_id)?
            .context("saved splat thumbnail was missing")?;
        assert_eq!(thumbnail.mime_type, "image/jpeg");
        assert_eq!(thumbnail.data, b"jpeg bytes");
        assert_eq!(
            thumbnail.camera_json.as_deref(),
            Some(r#"{"version":1,"position":[1,2,3],"focus":[4,5,6]}"#)
        );

        db.delete_image(image_id)?;
        let count =
            db.connection
                .query_row("SELECT COUNT(*) FROM splat_thumbnails", [], |row| {
                    row.get::<_, i64>(0)
                })?;
        assert_eq!(count, 0);

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn splat_thumbnail_is_removed_when_source_timestamp_changes() -> Result<()> {
        let root = temp_root_path("splat_thumbnail_is_removed_when_source_timestamp_changes");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root)?;
        fs::write(root.join("scene.spz"), b"splat bytes")?;

        let mut db = RootDatabase::open(&root)?;
        let root_id = db.root_id()?;
        db.scan(&root_id)?;
        let image_id = db
            .images_for_folder(&root_id, "")?
            .first()
            .map(|image| image.id)
            .context("splat was not indexed")?;

        db.save_splat_thumbnail(image_id, "image/jpeg", b"jpeg bytes", None)?;
        db.connection.execute(
            "UPDATE images SET modified_unix_ms = modified_unix_ms + 1 WHERE id = ?1",
            params![image_id],
        )?;

        assert!(db.splat_thumbnail(image_id)?.is_none());
        let count =
            db.connection
                .query_row("SELECT COUNT(*) FROM splat_thumbnails", [], |row| {
                    row.get::<_, i64>(0)
                })?;
        assert_eq!(count, 0);

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn folder_metadata_inherits_people_and_rating_from_ancestors() -> Result<()> {
        let root = temp_root_path("folder_metadata_inherits_people_and_rating_from_ancestors");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("Ada").join("Set"))?;
        fs::write(
            root.join("Ada").join("Set").join("portrait.jpg"),
            b"image bytes",
        )?;

        let mut db = RootDatabase::open(&root)?;
        let root_id = db.root_id()?;
        db.scan(&root_id)?;
        let parent_id = db.folder_id("Ada")?;
        let child_id = db.folder_id("Ada/Set")?;

        let parent_metadata = db.add_folder_person(&root_id, parent_id, "Ada Lovelace")?;
        assert_eq!(parent_metadata.people.len(), 1);
        assert!(parent_metadata.inherited_people.is_empty());

        db.set_folder_rating(&root_id, parent_id, Some(5))?;
        db.add_folder_keyword(&root_id, parent_id, "portrait")?;

        let child_metadata = db.folder_metadata(&root_id, child_id)?;
        assert!(child_metadata.people.is_empty());
        assert_eq!(child_metadata.inherited_people.len(), 1);
        assert_eq!(child_metadata.inherited_people[0].name, "Ada Lovelace");
        assert!(child_metadata.tags.is_empty());
        assert_eq!(child_metadata.inherited_tags.len(), 1);
        assert_eq!(child_metadata.inherited_tags[0].name, "portrait");
        assert_eq!(child_metadata.rating, None);
        assert_eq!(child_metadata.inherited_rating, Some(5));

        let parent_view = db.folder_view(&root_id, "Root", "Ada")?;
        let child_summary = parent_view
            .folders
            .iter()
            .find(|folder| folder.relative_path == "Ada/Set")
            .context("child folder was not visible")?;
        assert!(child_summary.direct_people.is_empty());
        assert_eq!(child_summary.inherited_people, vec!["Ada Lovelace"]);
        assert!(child_summary.direct_keywords.is_empty());
        assert_eq!(child_summary.inherited_keywords, vec!["portrait"]);
        assert_eq!(child_summary.direct_rating, None);
        assert_eq!(child_summary.inherited_rating, Some(5));

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn metadata_catalog_rename_merges_and_delete_removes_references() -> Result<()> {
        let root = temp_root_path("metadata_catalog_rename_merges_and_delete_removes_references");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("Ada"))?;
        fs::create_dir_all(root.join("Bea"))?;
        fs::write(root.join("Ada").join("portrait.jpg"), b"image bytes")?;
        fs::write(root.join("Bea").join("portrait.jpg"), b"image bytes")?;

        let mut db = RootDatabase::open(&root)?;
        let root_id = db.root_id()?;
        db.scan(&root_id)?;
        let ada_id = db.folder_id("Ada")?;
        let bea_id = db.folder_id("Bea")?;

        db.add_folder_person(&root_id, ada_id, "Ada")?;
        db.add_folder_person(&root_id, bea_id, "Bea")?;
        db.rename_person("Ada", "Bea")?;
        assert_eq!(
            db.people()?
                .into_iter()
                .map(|person| person.name)
                .collect::<Vec<_>>(),
            vec!["Bea"]
        );
        assert_eq!(db.folder_metadata(&root_id, ada_id)?.people[0].name, "Bea");
        assert_eq!(db.folder_metadata(&root_id, bea_id)?.people[0].name, "Bea");

        db.add_folder_keyword(&root_id, ada_id, "private")?;
        db.add_folder_keyword(&root_id, bea_id, "favorite")?;
        db.rename_keyword("private", "favorite")?;
        assert_eq!(
            db.keywords()?
                .into_iter()
                .map(|keyword| keyword.name)
                .collect::<Vec<_>>(),
            vec!["favorite"]
        );
        assert_eq!(
            db.folder_metadata(&root_id, ada_id)?.tags[0].name,
            "favorite"
        );
        assert_eq!(
            db.folder_metadata(&root_id, bea_id)?.tags[0].name,
            "favorite"
        );

        db.delete_keyword("favorite")?;
        assert!(db.keywords()?.is_empty());
        assert!(db.folder_metadata(&root_id, ada_id)?.tags.is_empty());
        assert!(db.folder_metadata(&root_id, bea_id)?.tags.is_empty());

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn folder_thumbnail_can_be_set_by_relative_path() -> Result<()> {
        let root = temp_root_path("folder_thumbnail_can_be_set_by_relative_path");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("Parent").join("Child"))?;
        fs::write(
            root.join("Parent").join("parent.jpg"),
            b"parent image bytes",
        )?;
        fs::write(
            root.join("Parent").join("Child").join("child.jpg"),
            b"child image bytes",
        )?;

        let mut db = RootDatabase::open(&root)?;
        let root_id = db.root_id()?;
        db.scan(&root_id)?;
        let child_image_id = db
            .images_for_folder(&root_id, "Parent/Child")?
            .first()
            .map(|image| image.id)
            .context("child image was not indexed")?;

        db.set_folder_thumbnail_by_path("Parent", child_image_id)?;
        let root_view = db.folder_view(&root_id, "Root", "")?;
        let parent_summary = root_view
            .folders
            .iter()
            .find(|folder| folder.relative_path == "Parent")
            .context("parent folder was not visible")?;
        assert_eq!(parent_summary.thumbnail_image_id, Some(child_image_id));

        db.set_folder_thumbnail_by_path("", child_image_id)?;
        assert_eq!(db.root_thumbnail_image_id()?, Some(child_image_id));

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn flattened_folder_summary_uses_visible_parent_for_parent_cover() -> Result<()> {
        let root = temp_root_path("flattened_folder_summary_uses_visible_parent_for_parent_cover");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("Outer").join("Inner"))?;
        fs::write(
            root.join("Outer").join("Inner").join("child.jpg"),
            b"child image bytes",
        )?;

        let mut db = RootDatabase::open(&root)?;
        let root_id = db.root_id()?;
        db.scan(&root_id)?;

        let root_view = db.folder_view(&root_id, "Root", "")?;
        assert_eq!(root_view.folders.len(), 1);
        let inner_summary = &root_view.folders[0];
        assert_eq!(inner_summary.relative_path, "Outer/Inner");
        assert_eq!(inner_summary.parent_relative_path.as_deref(), Some(""));

        let image_id = inner_summary
            .thumbnail_image_id
            .context("inner folder thumbnail was not available")?;
        db.set_folder_thumbnail_by_path(
            inner_summary
                .parent_relative_path
                .as_deref()
                .context("visible parent path was not available")?,
            image_id,
        )?;
        assert_eq!(db.root_thumbnail_image_id()?, Some(image_id));

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn metadata_search_uses_inherited_filters_and_exclude_modes() -> Result<()> {
        let root = temp_root_path("metadata_search_uses_inherited_filters_and_exclude_modes");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("Ada").join("Keep"))?;
        fs::create_dir_all(root.join("Ada").join("Reject"))?;
        fs::create_dir_all(root.join("Bea").join("Keep"))?;
        fs::write(
            root.join("Ada").join("Keep").join("portrait.jpg"),
            b"image bytes",
        )?;
        fs::write(
            root.join("Ada").join("Reject").join("portrait.jpg"),
            b"image bytes",
        )?;
        fs::write(
            root.join("Bea").join("Keep").join("portrait.jpg"),
            b"image bytes",
        )?;

        let mut db = RootDatabase::open(&root)?;
        let root_id = db.root_id()?;
        db.scan(&root_id)?;

        let ada_id = db.folder_id("Ada")?;
        let ada_keep_id = db.folder_id("Ada/Keep")?;
        let ada_reject_id = db.folder_id("Ada/Reject")?;
        let bea_id = db.folder_id("Bea")?;
        db.add_folder_person(&root_id, ada_id, "Ada Lovelace")?;
        db.add_folder_keyword(&root_id, ada_id, "portrait")?;
        db.set_folder_rating(&root_id, ada_id, Some(5))?;
        db.add_folder_keyword(&root_id, ada_keep_id, "blurry")?;
        db.add_folder_keyword(&root_id, ada_reject_id, "blurry")?;
        db.add_folder_keyword(&root_id, ada_reject_id, "private")?;
        db.add_folder_person(&root_id, bea_id, "Bea")?;
        db.add_folder_keyword(&root_id, bea_id, "portrait")?;
        db.set_folder_rating(&root_id, bea_id, Some(2))?;

        let tag_query = MetadataSearchQuery {
            include_tags: MetadataNameFilter {
                names: vec!["portrait".to_owned()],
                combine: MetadataCombineMode::And,
            },
            ..Default::default()
        };
        let paths = db
            .search_folders(&root_id, &tag_query)?
            .into_iter()
            .map(|folder| folder.relative_path)
            .collect::<Vec<_>>();
        assert!(paths.contains(&"Ada".to_owned()));
        assert!(paths.contains(&"Ada/Keep".to_owned()));
        assert!(paths.contains(&"Ada/Reject".to_owned()));
        assert!(paths.contains(&"Bea".to_owned()));
        assert!(paths.contains(&"Bea/Keep".to_owned()));

        let tag_query = MetadataSearchQuery {
            include_tags: MetadataNameFilter {
                names: vec!["portrait".to_owned(), "blurry".to_owned()],
                combine: MetadataCombineMode::And,
            },
            ..Default::default()
        };
        let paths = db
            .search_folders(&root_id, &tag_query)?
            .into_iter()
            .map(|folder| folder.relative_path)
            .collect::<Vec<_>>();
        assert!(!paths.contains(&"Ada".to_owned()));
        assert!(paths.contains(&"Ada/Keep".to_owned()));
        assert!(paths.contains(&"Ada/Reject".to_owned()));
        assert!(!paths.contains(&"Bea".to_owned()));
        assert!(!paths.contains(&"Bea/Keep".to_owned()));

        let rating_query = MetadataSearchQuery {
            minimum_rating: Some(5),
            ..Default::default()
        };
        let paths = db
            .search_folders(&root_id, &rating_query)?
            .into_iter()
            .map(|folder| folder.relative_path)
            .collect::<Vec<_>>();
        assert!(paths.contains(&"Ada".to_owned()));
        assert!(paths.contains(&"Ada/Keep".to_owned()));
        assert!(paths.contains(&"Ada/Reject".to_owned()));
        assert!(!paths.contains(&"Bea".to_owned()));
        assert!(!paths.contains(&"Bea/Keep".to_owned()));

        let query = MetadataSearchQuery {
            person: Some("Ada Lovelace".to_owned()),
            include_tags: MetadataNameFilter {
                names: vec!["portrait".to_owned()],
                combine: MetadataCombineMode::And,
            },
            exclude_tags: MetadataNameFilter {
                names: vec!["blurry".to_owned(), "private".to_owned()],
                combine: MetadataCombineMode::And,
            },
            minimum_rating: Some(4),
        };

        let paths = db
            .search_folders(&root_id, &query)?
            .into_iter()
            .map(|folder| folder.relative_path)
            .collect::<Vec<_>>();
        assert!(paths.contains(&"Ada".to_owned()));
        assert!(paths.contains(&"Ada/Keep".to_owned()));
        assert!(!paths.contains(&"Ada/Reject".to_owned()));
        assert!(!paths.contains(&"Bea".to_owned()));

        let query = MetadataSearchQuery {
            exclude_tags: MetadataNameFilter {
                names: vec!["blurry".to_owned()],
                combine: MetadataCombineMode::Or,
            },
            ..query
        };
        let paths = db
            .search_folders(&root_id, &query)?
            .into_iter()
            .map(|folder| folder.relative_path)
            .collect::<Vec<_>>();
        assert!(paths.contains(&"Ada".to_owned()));
        assert!(!paths.contains(&"Ada/Keep".to_owned()));
        assert!(!paths.contains(&"Ada/Reject".to_owned()));

        let people = db.person_summaries(&root_id)?;
        let ada = people
            .iter()
            .find(|person| person.name == "Ada Lovelace")
            .context("Ada person summary was missing")?;
        assert_eq!(ada.folder_count, 2);
        assert!(ada.thumbnail_image_id.is_some());

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn delete_folder_removes_indexed_subtree() -> Result<()> {
        let root = temp_root_path("delete_folder_removes_indexed_subtree");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("Ada").join("Set"))?;
        fs::write(
            root.join("Ada").join("Set").join("portrait.jpg"),
            b"image bytes",
        )?;

        let mut db = RootDatabase::open(&root)?;
        let root_id = db.root_id()?;
        db.scan(&root_id)?;
        let folder_id = db.folder_id("Ada")?;
        db.add_folder_person(&root_id, folder_id, "Ada Lovelace")?;

        assert_eq!(db.folder_path("Ada/Set")?, root.join("Ada").join("Set"));
        assert_eq!(db.recursive_images_for_folder(&root_id, "Ada")?.len(), 1);

        db.delete_folder("Ada")?;

        assert!(db.folder_path("Ada").is_err());
        assert!(db.recursive_images_for_folder(&root_id, "Ada")?.is_empty());
        assert!(db.folder_metadata(&root_id, folder_id).is_err());

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn rescan_removes_deleted_child_folder_from_view() -> Result<()> {
        let root = temp_root_path("rescan_removes_deleted_child_folder_from_view");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("Parent").join("Child"))?;
        fs::write(
            root.join("Parent").join("Child").join("portrait.jpg"),
            b"image bytes",
        )?;

        let mut db = RootDatabase::open(&root)?;
        let root_id = db.root_id()?;
        db.scan(&root_id)?;
        assert!(db.folder_path("Parent/Child").is_ok());

        fs::remove_dir_all(root.join("Parent").join("Child"))?;
        db.rescan_with_progress(&root_id, "Parent", |_| {})?;

        let view = db.folder_view(&root_id, "Root", "Parent")?;
        assert!(view.folders.is_empty());
        assert!(view.images.is_empty());
        assert!(db.folder_path("Parent/Child").is_err());

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn image_metadata_migrates_to_containing_folder() -> Result<()> {
        let root = temp_root_path("image_metadata_migrates_to_containing_folder");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("Ada"))?;
        fs::write(root.join("Ada").join("portrait.jpg"), b"image bytes")?;

        let mut db = RootDatabase::open(&root)?;
        let root_id = db.root_id()?;
        db.scan(&root_id)?;
        let folder_id = db.folder_id("Ada")?;
        let image_id = db
            .images_for_folder(&root_id, "Ada")?
            .first()
            .map(|image| image.id)
            .context("test image was not indexed")?;

        db.add_image_person(&root_id, image_id, "Ada Lovelace")?;
        db.set_image_rating(&root_id, image_id, Some(3))?;
        drop(db);

        let db = RootDatabase::open_existing(&root)?.context("test database was not reopened")?;
        let metadata = db.folder_metadata(&root_id, folder_id)?;
        assert_eq!(metadata.people.len(), 1);
        assert_eq!(metadata.people[0].name, "Ada Lovelace");
        assert_eq!(metadata.rating, Some(3));

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn legacy_smiley_ratings_are_dropped() -> Result<()> {
        let root = temp_root_path("legacy_smiley_ratings_are_dropped");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("Ada"))?;
        fs::write(root.join("Ada").join("portrait.jpg"), b"image bytes")?;

        let mut db = RootDatabase::open(&root)?;
        let root_id = db.root_id()?;
        db.scan(&root_id)?;
        let folder_id = db.folder_id("Ada")?;
        let image_id = db
            .images_for_folder(&root_id, "Ada")?
            .first()
            .map(|image| image.id)
            .context("test image was not indexed")?;

        db.connection.execute("DELETE FROM ratings", [])?;
        for (id, name) in [(1_i64, "unhappy"), (2, "neutral"), (3, "happy")] {
            db.connection.execute(
                "INSERT INTO ratings(id, name) VALUES(?1, ?2)",
                params![id, name],
            )?;
        }
        db.connection.execute(
            "INSERT INTO folder_ratings(folder_id, rating_id) VALUES(?1, 3)",
            params![folder_id],
        )?;
        db.connection.execute(
            "INSERT INTO image_ratings(image_id, rating_id) VALUES(?1, 1)",
            params![image_id],
        )?;
        drop(db);

        let db = RootDatabase::open_existing(&root)?.context("test database was not reopened")?;
        assert_eq!(db.folder_metadata(&root_id, folder_id)?.rating, None);
        assert_eq!(db.image_metadata(&root_id, image_id)?.rating, None);

        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    fn temp_root_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("picturious-{name}-{}", Uuid::new_v4()))
    }
}
