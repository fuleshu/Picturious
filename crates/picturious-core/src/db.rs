use crate::models::{
    FolderSummary, FolderView, FolderViewHeader, ImageSummary, ScanProgress, ScanReport,
};
use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const DB_DIR: &str = ".picturious";
const DB_FILE: &str = "root.sqlite";
const ROOT_RELATIVE_PATH: &str = "";
const SCHEMA_VERSION: &str = "2";
const SUPPORTED_IMAGE_EXTENSIONS: &[&str] = &[
    "jpg", "jpeg", "png", "webp", "gif", "bmp", "tif", "tiff", "avif",
];

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
    should_descend: bool,
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

            if validation.should_descend {
                for child_relative_path in &validation.child_relative_paths {
                    pending_dirs.push_back(child_relative_path.clone());
                }
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
        const BATCH_SIZE: usize = 1;

        let normalized_relative_path = normalize_relative_path(relative_path);
        let mut folder_batch = Vec::with_capacity(BATCH_SIZE);
        self.for_each_direct_child_folder_row(&normalized_relative_path, |row| {
            for visible_row in self.visible_folder_rows_from(row, 1)? {
                folder_batch.push(self.folder_summary(
                    root_id,
                    &normalized_relative_path,
                    visible_row.id,
                    visible_row.relative_path,
                    visible_row.parent_relative_path,
                    visible_row.selected_thumbnail_image_id,
                    visible_row.image_count,
                    visible_row.child_folder_count,
                )?);
                if folder_batch.len() >= BATCH_SIZE {
                    on_batch(std::mem::take(&mut folder_batch), Vec::new())?;
                }
            }
            Ok(())
        })?;
        if !folder_batch.is_empty() {
            on_batch(std::mem::take(&mut folder_batch), Vec::new())?;
        }

        let mut image_batch = Vec::with_capacity(BATCH_SIZE);
        self.for_each_image_for_folder(root_id, &normalized_relative_path, |image| {
            image_batch.push(image);
            if image_batch.len() >= BATCH_SIZE {
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
                should_descend: false,
                child_relative_paths: Vec::new(),
                image_count: 0,
                skipped_entries: 1,
            });
        }

        let snapshot = read_directory_snapshot(&self.root_path, &folder_path)?;
        let validation_started = unix_time_ms(SystemTime::now());
        let parent_path = parent_relative_path(&normalized_relative_path);
        let previous_hash = self.folder_content_hash(&normalized_relative_path);
        let existing_folder_id = self.folder_id_optional(&normalized_relative_path)?;
        let existing_images = if let Some(folder_id) = existing_folder_id {
            self.direct_image_rows(folder_id)?
        } else {
            Vec::new()
        };
        let existing_children = self.direct_child_relative_paths(&normalized_relative_path)?;
        let child_hashes_missing =
            self.direct_child_has_unvalidated_folder(&normalized_relative_path);
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

        let hash_changed = previous_hash.as_deref() != Some(snapshot.content_hash.as_str());
        let changed = !same_image_entries(&existing_images, &snapshot.images)
            || existing_children != scanned_child_paths
            || existing_folder_id.is_none();
        let should_descend = changed || hash_changed || child_hashes_missing;

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
            should_descend,
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
            ",
        )?;

        self.ensure_scan_columns()?;

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
                    row.parent_relative_path,
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

    fn folder_content_hash(&self, relative_path: &str) -> Option<String> {
        if self.ensure_scan_columns().is_err() {
            return None;
        }

        self.connection
            .query_row(
                "SELECT content_hash FROM folders WHERE relative_path = ?1",
                params![relative_path],
                |row| row.get(0),
            )
            .optional()
            .ok()
            .flatten()
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

    fn direct_child_has_unvalidated_folder(&self, parent_relative_path: &str) -> bool {
        if self.ensure_scan_columns().is_err() {
            return true;
        }

        self.connection
            .query_row(
                "
            SELECT EXISTS(
                SELECT 1
                FROM folders
                WHERE parent_relative_path = ?1
                    AND (content_hash IS NULL OR validated_at_unix_ms = 0)
            )
            ",
                params![parent_relative_path],
                |row| row.get::<_, i64>(0),
            )
            .map(|has_unvalidated| has_unvalidated != 0)
            .unwrap_or(true)
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

                if file_name.eq_ignore_ascii_case(DB_DIR) && file_type.is_dir() {
                    continue;
                }

                if file_type.is_dir() {
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

                if !file_type.is_file() || !is_supported_image(&path) {
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

fn names_for_folder(connection: &Connection, sql: &str, folder_id: i64) -> Result<Vec<String>> {
    let mut statement = connection.prepare(sql)?;
    let names = statement
        .query_map(params![folder_id], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(names)
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
