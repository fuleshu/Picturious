use crate::models::ThumbnailResponse;
use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use image::codecs::jpeg::JpegEncoder;
use image::metadata::Orientation;
use image::{DynamicImage, ExtendedColorType, ImageDecoder, ImageReader, RgbImage};
use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::path::Path;
use turbojpeg::{
    Compressor, Decompressor, Image as TurboImage, PixelFormat, ScalingFactor, Subsamp,
};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct ThumbnailKey {
    path: String,
    modified_unix_ms: i64,
    size: u32,
}

#[derive(Debug, Clone)]
pub struct GeneratedThumbnail {
    data_url: String,
    byte_len: usize,
}

pub struct ThumbnailCache {
    max_bytes: usize,
    total_bytes: usize,
    entries: HashMap<ThumbnailKey, GeneratedThumbnail>,
    order: VecDeque<ThumbnailKey>,
}

impl ThumbnailCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            total_bytes: 0,
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub fn get(
        &mut self,
        image_id: i64,
        path: &Path,
        modified_unix_ms: i64,
        size: u32,
    ) -> Option<ThumbnailResponse> {
        let key = thumbnail_key(path, modified_unix_ms, size);

        if let Some(cached) = self.entries.get(&key).cloned() {
            self.touch(&key);
            return Some(ThumbnailResponse {
                image_id,
                data_url: cached.data_url,
                from_cache: true,
            });
        }

        None
    }

    pub fn insert_generated(
        &mut self,
        path: &Path,
        modified_unix_ms: i64,
        size: u32,
        thumbnail: GeneratedThumbnail,
    ) {
        let key = thumbnail_key(path, modified_unix_ms, size);
        self.insert(key, thumbnail);
    }

    fn touch(&mut self, key: &ThumbnailKey) {
        self.order.retain(|existing| existing != key);
        self.order.push_back(key.clone());
    }

    fn insert(&mut self, key: ThumbnailKey, thumbnail: GeneratedThumbnail) {
        if let Some(existing) = self.entries.remove(&key) {
            self.total_bytes = self.total_bytes.saturating_sub(existing.byte_len);
            self.order.retain(|existing_key| existing_key != &key);
        }

        self.total_bytes += thumbnail.byte_len;
        self.order.push_back(key.clone());
        self.entries.insert(key, thumbnail);
        self.evict();
    }

    fn evict(&mut self) {
        while self.total_bytes > self.max_bytes {
            let Some(key) = self.order.pop_front() else {
                break;
            };

            if let Some(removed) = self.entries.remove(&key) {
                self.total_bytes = self.total_bytes.saturating_sub(removed.byte_len);
            }
        }
    }
}

impl Default for ThumbnailCache {
    fn default() -> Self {
        Self::new(192 * 1024 * 1024)
    }
}

impl GeneratedThumbnail {
    pub fn response(&self, image_id: i64, from_cache: bool) -> ThumbnailResponse {
        ThumbnailResponse {
            image_id,
            data_url: self.data_url.clone(),
            from_cache,
        }
    }
}

pub fn generate_thumbnail(path: &Path, size: u32) -> Result<GeneratedThumbnail> {
    let size = size.clamp(64, 2400);
    if is_jpeg(path) {
        return generate_jpeg_thumbnail(path, size)
            .or_else(|_| generate_generic_thumbnail(path, size));
    }

    generate_generic_thumbnail(path, size)
}

fn generate_jpeg_thumbnail(path: &Path, size: u32) -> Result<GeneratedThumbnail> {
    let orientation = image_orientation(path)?;
    let jpeg_data = std::fs::read(path)?;
    let mut decompressor = Decompressor::new()?;
    let header = decompressor.read_header(&jpeg_data)?;
    let scaling_factor = choose_scaling_factor(&header, size as usize);
    decompressor.set_scaling_factor(scaling_factor)?;
    let scaled_header = header.scaled(scaling_factor);
    let pitch = scaled_header.width * PixelFormat::RGB.size();

    let mut decoded = TurboImage {
        pixels: vec![0; pitch * scaled_header.height],
        width: scaled_header.width,
        pitch,
        height: scaled_header.height,
        format: PixelFormat::RGB,
    };
    decompressor.decompress(&jpeg_data, decoded.as_deref_mut())?;

    let rgb = RgbImage::from_raw(
        scaled_header.width as u32,
        scaled_header.height as u32,
        decoded.pixels,
    )
    .context("turbojpeg produced an invalid RGB buffer")?;

    let mut image = DynamicImage::ImageRgb8(rgb);
    image.apply_orientation(orientation);
    let thumbnail = image.thumbnail(size, size).to_rgb8();
    let bytes = encode_rgb_jpeg(thumbnail.as_raw(), thumbnail.width(), thumbnail.height())?;

    Ok(GeneratedThumbnail {
        byte_len: bytes.len(),
        data_url: jpeg_data_url(&bytes),
    })
}

fn generate_generic_thumbnail(path: &Path, size: u32) -> Result<GeneratedThumbnail> {
    let image = decode_image_with_orientation(path)?;
    let thumbnail = image.thumbnail(size, size).to_rgb8();
    let bytes = encode_rgb_jpeg(thumbnail.as_raw(), thumbnail.width(), thumbnail.height())?;

    Ok(GeneratedThumbnail {
        byte_len: bytes.len(),
        data_url: jpeg_data_url(&bytes),
    })
}

fn image_orientation(path: &Path) -> Result<Orientation> {
    let reader = ImageReader::open(path)?.with_guessed_format()?;
    let mut decoder = reader.into_decoder()?;
    Ok(decoder.orientation()?)
}

fn decode_image_with_orientation(path: &Path) -> Result<DynamicImage> {
    let reader = ImageReader::open(path)?.with_guessed_format()?;
    let mut decoder = reader.into_decoder()?;
    let orientation = decoder.orientation()?;
    let mut image = DynamicImage::from_decoder(decoder)?;
    image.apply_orientation(orientation);
    Ok(image)
}

fn thumbnail_key(path: &Path, modified_unix_ms: i64, size: u32) -> ThumbnailKey {
    ThumbnailKey {
        path: path.to_string_lossy().to_string(),
        modified_unix_ms,
        size: size.clamp(64, 2400),
    }
}

fn encode_rgb_jpeg(rgb: &[u8], width: u32, height: u32) -> Result<Vec<u8>> {
    if let Ok(bytes) = encode_rgb_jpeg_turbo(rgb, width, height) {
        return Ok(bytes);
    }

    let mut jpeg = Cursor::new(Vec::new());
    let mut encoder = JpegEncoder::new_with_quality(&mut jpeg, 82);
    encoder.encode(rgb, width, height, ExtendedColorType::Rgb8)?;
    Ok(jpeg.into_inner())
}

fn encode_rgb_jpeg_turbo(rgb: &[u8], width: u32, height: u32) -> Result<Vec<u8>> {
    let mut compressor = Compressor::new()?;
    compressor.set_quality(82)?;
    compressor.set_subsamp(Subsamp::Sub2x2)?;
    let image = TurboImage {
        pixels: rgb,
        width: width as usize,
        pitch: width as usize * PixelFormat::RGB.size(),
        height: height as usize,
        format: PixelFormat::RGB,
    };
    Ok(compressor.compress_to_vec(image)?)
}

fn choose_scaling_factor(
    header: &turbojpeg::DecompressHeader,
    target_size: usize,
) -> ScalingFactor {
    let max_dimension = header.width.max(header.height);
    if header.is_lossless || max_dimension <= target_size {
        return ScalingFactor::ONE;
    }

    let mut factors = Decompressor::supported_scaling_factors()
        .into_iter()
        .filter(|factor| factor.num() <= factor.denom())
        .collect::<Vec<_>>();
    factors.sort_by_key(|factor| factor.scale(max_dimension));

    factors
        .into_iter()
        .find(|factor| factor.scale(max_dimension) >= target_size)
        .unwrap_or(ScalingFactor::ONE)
}

fn is_jpeg(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            extension.eq_ignore_ascii_case("jpg") || extension.eq_ignore_ascii_case("jpeg")
        })
        .unwrap_or(false)
}

fn jpeg_data_url(bytes: &[u8]) -> String {
    format!("data:image/jpeg;base64,{}", STANDARD.encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{ImageBuffer, ImageEncoder, Rgb};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn jpeg_thumbnail_preserves_aspect_ratio() {
        let path = temp_image_path("jpg");
        let source = ImageBuffer::from_pixel(800, 400, Rgb([80, 120, 180]));
        let mut jpeg = Cursor::new(Vec::new());
        let mut encoder = JpegEncoder::new_with_quality(&mut jpeg, 90);
        encoder
            .encode(source.as_raw(), 800, 400, ExtendedColorType::Rgb8)
            .unwrap();
        fs::write(&path, jpeg.into_inner()).unwrap();

        let thumbnail = generate_thumbnail(&path, 300).unwrap();
        let encoded = thumbnail
            .data_url
            .strip_prefix("data:image/jpeg;base64,")
            .unwrap();
        let bytes = STANDARD.decode(encoded).unwrap();
        let decoded = ImageReader::new(Cursor::new(bytes))
            .with_guessed_format()
            .unwrap()
            .decode()
            .unwrap();

        fs::remove_file(path).ok();
        assert_eq!((decoded.width(), decoded.height()), (300, 150));
    }

    #[test]
    fn jpeg_thumbnail_applies_exif_orientation() {
        let path = temp_image_path("jpg");
        let source = ImageBuffer::from_pixel(800, 400, Rgb([180, 120, 80]));
        let mut jpeg = Cursor::new(Vec::new());
        let mut encoder = JpegEncoder::new_with_quality(&mut jpeg, 90);
        encoder
            .set_exif_metadata(exif_orientation_chunk(6))
            .unwrap();
        encoder
            .encode(source.as_raw(), 800, 400, ExtendedColorType::Rgb8)
            .unwrap();
        fs::write(&path, jpeg.into_inner()).unwrap();

        let thumbnail = generate_thumbnail(&path, 300).unwrap();
        let decoded = decode_thumbnail_response(&thumbnail);

        fs::remove_file(path).ok();
        assert_eq!((decoded.width(), decoded.height()), (150, 300));
    }

    fn decode_thumbnail_response(thumbnail: &GeneratedThumbnail) -> DynamicImage {
        let encoded = thumbnail
            .data_url
            .strip_prefix("data:image/jpeg;base64,")
            .unwrap();
        let bytes = STANDARD.decode(encoded).unwrap();
        ImageReader::new(Cursor::new(bytes))
            .with_guessed_format()
            .unwrap()
            .decode()
            .unwrap()
    }

    fn exif_orientation_chunk(orientation: u16) -> Vec<u8> {
        let mut chunk = Vec::new();
        chunk.extend_from_slice(b"II*\0");
        chunk.extend_from_slice(&8_u32.to_le_bytes());
        chunk.extend_from_slice(&1_u16.to_le_bytes());
        chunk.extend_from_slice(&0x0112_u16.to_le_bytes());
        chunk.extend_from_slice(&3_u16.to_le_bytes());
        chunk.extend_from_slice(&1_u32.to_le_bytes());
        chunk.extend_from_slice(&orientation.to_le_bytes());
        chunk.extend_from_slice(&0_u16.to_le_bytes());
        chunk.extend_from_slice(&0_u32.to_le_bytes());
        chunk
    }

    fn temp_image_path(extension: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "picturious-thumb-test-{}-{nanos}.{extension}",
            std::process::id()
        ))
    }
}
