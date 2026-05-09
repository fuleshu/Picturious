use crate::db::{
    RootDatabase, StoredSplatThumbnail, clean_path_string, root_database_exists,
    root_database_path, root_display_name, validate_root_path,
};
use crate::models::{
    FolderMetadata, FolderSummary, FolderView, ImageMetadata, ImageSummary, LibraryOverview,
    LibraryRoot, MetadataPersonSummary, MetadataSearchQuery, MetadataTag, ScanProgress, ScanReport,
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
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
    pub relative_path: String,
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
        self.scan_folder_with_progress(root_id, "", on_progress)
    }

    pub fn scan_folder_with_progress<F>(
        &self,
        root_id: &str,
        relative_path: &str,
        on_progress: F,
    ) -> Result<ScanReport>
    where
        F: FnMut(ScanProgress),
    {
        let known_root = self.known_root(root_id)?;
        let mut db = self.open_connected_database(known_root)?;
        db.rescan_with_progress(root_id, relative_path, on_progress)
    }

    pub fn scan_target(&self, root_id: &str, relative_path: &str) -> Result<ScanTarget> {
        let known_root = self.known_root(root_id)?;
        self.open_connected_database(known_root)?;
        Ok(ScanTarget {
            root_id: known_root.id.clone(),
            path: PathBuf::from(&known_root.path),
            relative_path: relative_path.to_owned(),
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

    pub fn splat_thumbnail(
        &self,
        root_id: &str,
        image_id: i64,
    ) -> Result<Option<StoredSplatThumbnail>> {
        self.asset_thumbnail(root_id, image_id)
    }

    pub fn asset_thumbnail(
        &self,
        root_id: &str,
        image_id: i64,
    ) -> Result<Option<StoredSplatThumbnail>> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.asset_thumbnail(image_id)
    }

    pub fn save_splat_thumbnail(
        &self,
        root_id: &str,
        image_id: i64,
        mime_type: &str,
        data: &[u8],
        camera_json: Option<&str>,
    ) -> Result<()> {
        self.save_asset_thumbnail(root_id, image_id, mime_type, data, camera_json)
    }

    pub fn save_asset_thumbnail(
        &self,
        root_id: &str,
        image_id: i64,
        mime_type: &str,
        data: &[u8],
        camera_json: Option<&str>,
    ) -> Result<()> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.save_asset_thumbnail(image_id, mime_type, data, camera_json)
    }

    pub fn folder_path(&self, root_id: &str, relative_path: &str) -> Result<PathBuf> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.folder_path(relative_path)
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

    pub fn delete_folder(&self, root_id: &str, relative_path: &str) -> Result<()> {
        let known_root = self.known_root(root_id)?;
        let mut db = self.open_connected_database(known_root)?;
        db.delete_folder(relative_path)
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

    pub fn set_folder_thumbnail_by_path(
        &mut self,
        root_id: &str,
        relative_path: &str,
        image_id: i64,
    ) -> Result<()> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.set_folder_thumbnail_by_path(relative_path, image_id)
    }

    pub fn image_metadata(&self, root_id: &str, image_id: i64) -> Result<ImageMetadata> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.image_metadata(root_id, image_id)
    }

    pub fn folder_metadata(&self, root_id: &str, folder_id: i64) -> Result<FolderMetadata> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.folder_metadata(root_id, folder_id)
    }

    pub fn people(&self, root_id: &str) -> Result<Vec<MetadataTag>> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.people()
    }

    pub fn keywords(&self, root_id: &str) -> Result<Vec<MetadataTag>> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.keywords()
    }

    pub fn all_people(&self) -> Result<Vec<MetadataTag>> {
        let mut names_by_key = BTreeMap::new();
        for root in &self.roots {
            let Ok(db) = self.open_connected_database(root) else {
                continue;
            };
            for person in db.people()? {
                names_by_key
                    .entry(person.name.to_lowercase())
                    .or_insert(person.name);
            }
        }

        Ok(names_by_key
            .into_values()
            .enumerate()
            .map(|(index, name)| MetadataTag {
                id: index as i64 + 1,
                name,
            })
            .collect())
    }

    pub fn all_keywords(&self) -> Result<Vec<MetadataTag>> {
        let mut names_by_key = BTreeMap::new();
        for root in &self.roots {
            let Ok(db) = self.open_connected_database(root) else {
                continue;
            };
            for keyword in db.keywords()? {
                names_by_key
                    .entry(keyword.name.to_lowercase())
                    .or_insert(keyword.name);
            }
        }

        Ok(names_by_key
            .into_values()
            .enumerate()
            .map(|(index, name)| MetadataTag {
                id: index as i64 + 1,
                name,
            })
            .collect())
    }

    pub fn rename_person_everywhere(&self, old_name: &str, new_name: &str) -> Result<()> {
        for root in &self.roots {
            let Ok(mut db) = self.open_connected_database(root) else {
                continue;
            };
            db.rename_person(old_name, new_name)?;
        }
        Ok(())
    }

    pub fn delete_person_everywhere(&self, name: &str) -> Result<()> {
        for root in &self.roots {
            let Ok(db) = self.open_connected_database(root) else {
                continue;
            };
            db.delete_person(name)?;
        }
        Ok(())
    }

    pub fn rename_keyword_everywhere(&self, old_name: &str, new_name: &str) -> Result<()> {
        for root in &self.roots {
            let Ok(mut db) = self.open_connected_database(root) else {
                continue;
            };
            db.rename_keyword(old_name, new_name)?;
        }
        Ok(())
    }

    pub fn delete_keyword_everywhere(&self, name: &str) -> Result<()> {
        for root in &self.roots {
            let Ok(db) = self.open_connected_database(root) else {
                continue;
            };
            db.delete_keyword(name)?;
        }
        Ok(())
    }

    pub fn search_metadata(&self, query: &MetadataSearchQuery) -> Result<Vec<FolderSummary>> {
        let mut folders = Vec::new();
        for root in &self.roots {
            let Ok(db) = self.open_connected_database(root) else {
                continue;
            };
            folders.extend(db.search_folders(&root.id, query)?);
        }

        folders.sort_by(|left, right| {
            let left_root = self
                .roots
                .iter()
                .find(|root| root.id == left.root_id)
                .map(|root| root.display_name.as_str())
                .unwrap_or(left.root_id.as_str());
            let right_root = self
                .roots
                .iter()
                .find(|root| root.id == right.root_id)
                .map(|root| root.display_name.as_str())
                .unwrap_or(right.root_id.as_str());
            left_root
                .to_lowercase()
                .cmp(&right_root.to_lowercase())
                .then_with(|| {
                    left.relative_path
                        .to_lowercase()
                        .cmp(&right.relative_path.to_lowercase())
                })
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(folders)
    }

    pub fn all_people_with_thumbnails(&self) -> Result<Vec<MetadataPersonSummary>> {
        let mut people_by_key = BTreeMap::<String, MetadataPersonSummary>::new();
        for root in &self.roots {
            let Ok(db) = self.open_connected_database(root) else {
                continue;
            };
            for person in db.person_summaries(&root.id)? {
                let key = person.name.to_lowercase();
                people_by_key
                    .entry(key)
                    .and_modify(|existing| {
                        existing.folder_count =
                            existing.folder_count.saturating_add(person.folder_count);
                        if existing.thumbnail_image_id.is_none() {
                            existing.root_id = person.root_id.clone();
                            existing.thumbnail_image_id = person.thumbnail_image_id;
                        }
                    })
                    .or_insert(person);
            }
        }

        Ok(people_by_key
            .into_values()
            .enumerate()
            .map(|(index, mut person)| {
                person.id = index as i64 + 1;
                person
            })
            .collect())
    }

    pub fn filtered_people_with_thumbnails(
        &self,
        query: &MetadataSearchQuery,
    ) -> Result<Vec<MetadataPersonSummary>> {
        let people = self.all_people_with_thumbnails()?;
        if !person_summary_query_has_filters(query) {
            return Ok(people);
        }

        let mut personless_query = query.clone();
        personless_query.person = None;
        let matching_folders = self.search_metadata(&personless_query)?;
        let final_folders = matching_folders
            .iter()
            .filter(|folder| folder.image_count > 0)
            .collect::<Vec<_>>();
        let folders = if final_folders.is_empty() {
            matching_folders.iter().collect::<Vec<_>>()
        } else {
            final_folders
        };

        let mut counts_by_person = HashMap::<String, u32>::new();
        for folder in folders {
            let mut folder_people = HashSet::new();
            for name in folder
                .direct_people
                .iter()
                .chain(folder.inherited_people.iter())
            {
                let key = normalized_person_summary_key(name);
                if !key.is_empty() {
                    folder_people.insert(key);
                }
            }

            for key in folder_people {
                let count = counts_by_person.entry(key).or_default();
                *count = count.saturating_add(1);
            }
        }

        Ok(people
            .into_iter()
            .filter_map(|mut person| {
                let key = normalized_person_summary_key(&person.name);
                let folder_count = counts_by_person.get(&key).copied()?;
                if folder_count == 0 {
                    return None;
                }
                person.folder_count = folder_count;
                Some(person)
            })
            .enumerate()
            .map(|(index, mut person)| {
                person.id = index as i64 + 1;
                person
            })
            .collect())
    }

    pub fn add_folder_person(
        &self,
        root_id: &str,
        folder_id: i64,
        name: &str,
    ) -> Result<FolderMetadata> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.add_folder_person(root_id, folder_id, name)
    }

    pub fn add_folder_keyword(
        &self,
        root_id: &str,
        folder_id: i64,
        name: &str,
    ) -> Result<FolderMetadata> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.add_folder_keyword(root_id, folder_id, name)
    }

    pub fn remove_folder_keyword(
        &self,
        root_id: &str,
        folder_id: i64,
        keyword_id: i64,
    ) -> Result<FolderMetadata> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.remove_folder_keyword(root_id, folder_id, keyword_id)
    }

    pub fn remove_folder_person(
        &self,
        root_id: &str,
        folder_id: i64,
        person_id: i64,
    ) -> Result<FolderMetadata> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.remove_folder_person(root_id, folder_id, person_id)
    }

    pub fn set_folder_rating(
        &self,
        root_id: &str,
        folder_id: i64,
        rating: Option<u8>,
    ) -> Result<FolderMetadata> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.set_folder_rating(root_id, folder_id, rating)
    }

    pub fn add_image_person(
        &self,
        root_id: &str,
        image_id: i64,
        name: &str,
    ) -> Result<ImageMetadata> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.add_image_person(root_id, image_id, name)
    }

    pub fn remove_image_person(
        &self,
        root_id: &str,
        image_id: i64,
        person_id: i64,
    ) -> Result<ImageMetadata> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.remove_image_person(root_id, image_id, person_id)
    }

    pub fn set_image_rating(
        &self,
        root_id: &str,
        image_id: i64,
        rating: Option<u8>,
    ) -> Result<ImageMetadata> {
        let known_root = self.known_root(root_id)?;
        let db = self.open_connected_database(known_root)?;
        db.set_image_rating(root_id, image_id, rating)
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

fn person_summary_query_has_filters(query: &MetadataSearchQuery) -> bool {
    !query.include_tags.names.is_empty()
        || !query.exclude_tags.names.is_empty()
        || query.minimum_rating.is_some()
}

fn normalized_person_summary_key(name: &str) -> String {
    name.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
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

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn all_metadata_terms_combine_connected_roots_by_name() -> Result<()> {
        let workspace = temp_path("all_metadata_terms_combine_connected_roots_by_name");
        let config_dir = workspace.join("config");
        let root_a = workspace.join("root-a");
        let root_b = workspace.join("root-b");
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&root_a)?;
        fs::create_dir_all(&root_b)?;

        let mut manager = LibraryManager::new(&config_dir)?;
        let root_a = manager.add_root(root_a.to_string_lossy().as_ref())?;
        let root_b = manager.add_root(root_b.to_string_lossy().as_ref())?;
        let root_a_folder_id = manager.folder_view(&root_a.id, "")?.folder_id;

        manager.add_folder_person(&root_a.id, root_a_folder_id, "Max")?;
        manager.add_folder_keyword(&root_a.id, root_a_folder_id, "favorite")?;

        assert_eq!(manager.people(&root_b.id)?, Vec::<MetadataTag>::new());
        assert_eq!(manager.keywords(&root_b.id)?, Vec::<MetadataTag>::new());
        assert_eq!(
            manager
                .all_people()?
                .into_iter()
                .map(|person| person.name)
                .collect::<Vec<_>>(),
            vec!["Max"]
        );
        assert_eq!(
            manager
                .all_keywords()?
                .into_iter()
                .map(|tag| tag.name)
                .collect::<Vec<_>>(),
            vec!["favorite"]
        );

        let _ = fs::remove_dir_all(&workspace);
        Ok(())
    }

    #[test]
    fn metadata_terms_can_be_edited_across_connected_roots() -> Result<()> {
        let workspace = temp_path("metadata_terms_can_be_edited_across_connected_roots");
        let config_dir = workspace.join("config");
        let root_a = workspace.join("root-a");
        let root_b = workspace.join("root-b");
        let _ = fs::remove_dir_all(&workspace);
        fs::create_dir_all(&root_a)?;
        fs::create_dir_all(&root_b)?;

        let mut manager = LibraryManager::new(&config_dir)?;
        let root_a = manager.add_root(root_a.to_string_lossy().as_ref())?;
        let root_b = manager.add_root(root_b.to_string_lossy().as_ref())?;
        let root_a_folder_id = manager.folder_view(&root_a.id, "")?.folder_id;
        let root_b_folder_id = manager.folder_view(&root_b.id, "")?.folder_id;

        manager.add_folder_person(&root_a.id, root_a_folder_id, "Max")?;
        manager.add_folder_person(&root_b.id, root_b_folder_id, "Max")?;
        manager.rename_person_everywhere("Max", "Ada")?;
        assert_eq!(
            manager
                .all_people()?
                .into_iter()
                .map(|person| person.name)
                .collect::<Vec<_>>(),
            vec!["Ada"]
        );
        assert_eq!(
            manager
                .folder_metadata(&root_a.id, root_a_folder_id)?
                .people[0]
                .name,
            "Ada"
        );
        assert_eq!(
            manager
                .folder_metadata(&root_b.id, root_b_folder_id)?
                .people[0]
                .name,
            "Ada"
        );

        manager.add_folder_keyword(&root_a.id, root_a_folder_id, "favorite")?;
        manager.add_folder_keyword(&root_b.id, root_b_folder_id, "favorite")?;
        manager.rename_keyword_everywhere("favorite", "keeper")?;
        assert_eq!(
            manager
                .all_keywords()?
                .into_iter()
                .map(|tag| tag.name)
                .collect::<Vec<_>>(),
            vec!["keeper"]
        );
        manager.delete_keyword_everywhere("keeper")?;
        assert!(manager.all_keywords()?.is_empty());
        assert!(
            manager
                .folder_metadata(&root_a.id, root_a_folder_id)?
                .tags
                .is_empty()
        );
        assert!(
            manager
                .folder_metadata(&root_b.id, root_b_folder_id)?
                .tags
                .is_empty()
        );

        let _ = fs::remove_dir_all(&workspace);
        Ok(())
    }

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("picturious-{name}-{}", Uuid::new_v4()))
    }
}
