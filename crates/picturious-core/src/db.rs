use crate::models::{
    FolderSummary, FolderView, FolderViewHeader, ImageSummary, ScanProgress, ScanReport,
};
use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const DB_DIR: &str = ".picturious";
const DB_FILE: &str = "root.sqlite";
const ROOT_RELATIVE_PATH: &str = "";
const SCAN_COMMIT_INTERVAL: u32 = 32;
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
}

struct FolderRow {
    id: i64,
    relative_path: String,
    parent_relative_path: Option<String>,
    selected_thumbnail_image_id: Option<i64>,
    image_count: u32,
    child_folder_count: u32,
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
        if initialize {
            db.init_schema()?;
        }
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
        let scan_started = unix_time_ms(SystemTime::now());
        let mut folders_seen = 0_u32;
        let mut images_seen = 0_u32;
        let mut skipped_entries = 0_u32;
        let root_path = self.root_path.clone();
        let mut pending_dirs = vec![root_path.clone()];
        let mut folders_since_commit = 0_u32;
        let mut tx = self.connection.transaction()?;

        while let Some(folder_path) = pending_dirs.pop() {
            let relative_path = relative_path_for(&root_path, &folder_path)?;
            let parent_relative_path = parent_relative_path(&relative_path);
            let mut images = Vec::new();

            match fs::read_dir(&folder_path) {
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
                            Ok(metadata) => metadata,
                            Err(_) => {
                                skipped_entries += 1;
                                continue;
                            }
                        };

                        if file_name.eq_ignore_ascii_case(DB_DIR) && file_type.is_dir() {
                            continue;
                        }

                        if file_type.is_dir() {
                            pending_dirs.push(path);
                            continue;
                        }

                        if !file_type.is_file() || !is_supported_image(&path) {
                            continue;
                        }

                        let image_relative_path = relative_path_for(&root_path, &path)?;

                        images.push(ScannedImage {
                            file_name,
                            relative_path: image_relative_path,
                        });
                    }
                }
                Err(_) => {
                    skipped_entries += 1;
                }
            }

            let folder_id = upsert_folder(
                &tx,
                &relative_path,
                parent_relative_path.as_deref(),
                scan_started,
            )?;

            for image in images {
                upsert_image(
                    &tx,
                    folder_id,
                    &image.file_name,
                    &image.relative_path,
                    0,
                    0,
                    None,
                    None,
                    scan_started,
                )?;
                images_seen += 1;
            }
            folders_seen += 1;
            folders_since_commit += 1;

            if folders_since_commit >= SCAN_COMMIT_INTERVAL {
                tx.commit()?;
                tx = self.connection.transaction()?;
                folders_since_commit = 0;
            }

            on_progress(ScanProgress {
                root_id: root_id.to_owned(),
                folders_seen,
                images_seen,
                skipped_entries,
                current_relative_path: relative_path,
            });
        }

        tx.execute(
            "DELETE FROM images WHERE scanned_at_unix_ms <> ?1",
            params![scan_started],
        )?;
        tx.execute(
            "DELETE FROM folders WHERE relative_path <> '' AND last_seen_scan_ms <> ?1",
            params![scan_started],
        )?;
        tx.commit()?;

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
            PRAGMA journal_mode = DELETE;

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

        self.connection.execute(
            "
            INSERT INTO meta(key, value)
            VALUES('schema_version', '1')
            ON CONFLICT(key) DO NOTHING
            ",
            [],
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
                image_count: row.get::<_, i64>(4)?.max(0) as u32,
                child_folder_count: row.get::<_, i64>(5)?.max(0) as u32,
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
        if depth > 64 || !self.folder_subtree_has_images(&row.relative_path)? {
            return Ok(Vec::new());
        }

        if row.image_count > 0 {
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
            name: display_name_for_relative_path(&relative_path),
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

    fn folder_subtree_has_images(&self, relative_path: &str) -> Result<bool> {
        let (lower_bound, upper_bound) = subtree_image_bounds(relative_path);
        let has_images = self.connection.query_row(
            "
            SELECT EXISTS(
                SELECT 1
                FROM images
                WHERE relative_path >= ?1 AND relative_path < ?2
            )
            ",
            params![lower_bound, upper_bound],
            |row| row.get::<_, i64>(0),
        )?;

        Ok(has_images != 0)
    }
}

fn upsert_folder(
    tx: &Transaction<'_>,
    relative_path: &str,
    parent_relative_path: Option<&str>,
    scan_started: i64,
) -> Result<i64> {
    tx.execute(
        "
        INSERT INTO folders(relative_path, parent_relative_path, last_seen_scan_ms)
        VALUES(?1, ?2, ?3)
        ON CONFLICT(relative_path) DO UPDATE SET
            parent_relative_path = excluded.parent_relative_path,
            last_seen_scan_ms = excluded.last_seen_scan_ms
        ",
        params![relative_path, parent_relative_path, scan_started],
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
