use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LibraryRoot {
    pub id: String,
    pub display_name: String,
    pub path: String,
    pub connected: bool,
    pub database_path: Option<String>,
    pub folder_count: u32,
    pub image_count: u32,
    pub thumbnail_image_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FolderSummary {
    pub root_id: String,
    pub id: i64,
    pub relative_path: String,
    pub name: String,
    pub parent_relative_path: Option<String>,
    pub thumbnail_image_id: Option<i64>,
    pub direct_keywords: Vec<String>,
    pub inherited_keywords: Vec<String>,
    pub direct_people: Vec<String>,
    pub inherited_people: Vec<String>,
    pub direct_rating: Option<u8>,
    pub inherited_rating: Option<u8>,
    pub image_count: u32,
    pub child_folder_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImageSummary {
    pub root_id: String,
    pub id: i64,
    pub folder_id: i64,
    pub file_name: String,
    pub relative_path: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub file_size: u64,
    pub modified_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataTag {
    pub id: i64,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FolderMetadata {
    pub root_id: String,
    pub folder_id: i64,
    pub relative_path: String,
    pub rating: Option<u8>,
    pub inherited_rating: Option<u8>,
    pub people: Vec<MetadataTag>,
    pub inherited_people: Vec<MetadataTag>,
    pub tags: Vec<MetadataTag>,
    pub inherited_tags: Vec<MetadataTag>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetadataCombineMode {
    And,
    Or,
}

impl Default for MetadataCombineMode {
    fn default() -> Self {
        Self::And
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MetadataNameFilter {
    #[serde(default)]
    pub names: Vec<String>,
    #[serde(default)]
    pub combine: MetadataCombineMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MetadataSearchQuery {
    #[serde(default)]
    pub person: Option<String>,
    #[serde(default)]
    pub include_tags: MetadataNameFilter,
    #[serde(default)]
    pub exclude_tags: MetadataNameFilter,
    #[serde(default)]
    pub minimum_rating: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataPersonSummary {
    pub id: i64,
    pub name: String,
    pub root_id: Option<String>,
    pub thumbnail_image_id: Option<i64>,
    pub folder_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImageMetadata {
    pub root_id: String,
    pub image_id: i64,
    pub rating: Option<u8>,
    pub people: Vec<MetadataTag>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FolderViewHeader {
    pub root_id: String,
    pub root_display_name: String,
    pub folder_id: i64,
    pub relative_path: String,
    pub parent_relative_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FolderView {
    pub root_id: String,
    pub root_display_name: String,
    pub folder_id: i64,
    pub relative_path: String,
    pub parent_relative_path: Option<String>,
    pub folders: Vec<FolderSummary>,
    pub images: Vec<ImageSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LibraryOverview {
    pub roots: Vec<LibraryRoot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScanReport {
    pub root_id: String,
    pub folders_seen: u32,
    pub images_seen: u32,
    pub skipped_entries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScanProgress {
    pub root_id: String,
    pub folders_seen: u32,
    pub images_seen: u32,
    pub skipped_entries: u32,
    pub current_relative_path: String,
    pub changed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ThumbnailResponse {
    pub image_id: i64,
    pub data_url: String,
    pub from_cache: bool,
}
