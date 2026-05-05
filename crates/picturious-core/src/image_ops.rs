use anyhow::{Context, Result, bail};
use image::codecs::jpeg::JpegEncoder;
use image::{ExtendedColorType, ImageReader, Rgb, RgbImage};
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use turbojpeg::{Transform, TransformOp};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationDirection {
    Left,
    Right,
}

pub fn rotate_image(path: &Path, direction: RotationDirection) -> Result<()> {
    if is_jpeg(path) {
        return rotate_jpeg_lossless(path, direction).or_else(|_| rotate_generic(path, direction));
    }

    rotate_generic(path, direction)
}

pub fn convert_png_to_jpg(path: &Path, quality: u8) -> Result<PathBuf> {
    if !is_png(path) {
        bail!("not a PNG image: {}", path.display());
    }

    let image = ImageReader::open(path)
        .with_context(|| format!("could not open {}", path.display()))?
        .with_guessed_format()
        .with_context(|| format!("could not detect image format for {}", path.display()))?
        .decode()
        .with_context(|| format!("could not decode {}", path.display()))?;
    let rgb = rgba_over_white_to_rgb(image.to_rgba8());
    let output_path = unique_jpg_path_for(path)?;
    let file = File::create(&output_path)
        .with_context(|| format!("could not write {}", output_path.display()))?;
    let mut writer = BufWriter::new(file);
    let mut encoder = JpegEncoder::new_with_quality(&mut writer, clamp_jpeg_quality(quality));
    encoder
        .encode(
            rgb.as_raw(),
            rgb.width(),
            rgb.height(),
            ExtendedColorType::Rgb8,
        )
        .with_context(|| format!("could not encode {}", output_path.display()))?;

    Ok(output_path)
}

pub fn image_dimensions(path: &Path) -> Result<(u32, u32)> {
    ImageReader::open(path)
        .with_context(|| format!("could not open {}", path.display()))?
        .with_guessed_format()
        .with_context(|| format!("could not detect image format for {}", path.display()))?
        .into_dimensions()
        .with_context(|| format!("could not read image dimensions for {}", path.display()))
}

fn rotate_jpeg_lossless(path: &Path, direction: RotationDirection) -> Result<()> {
    let jpeg_data = fs::read(path).with_context(|| format!("could not read {}", path.display()))?;
    let op = match direction {
        RotationDirection::Left => TransformOp::Rot270,
        RotationDirection::Right => TransformOp::Rot90,
    };
    let mut transform = Transform::op(op);
    transform.trim = true;
    transform.optimize = true;
    let rotated = turbojpeg::transform(&transform, &jpeg_data)
        .with_context(|| format!("could not rotate JPEG {}", path.display()))?;
    write_bytes_via_temp(path, rotated.as_ref())
}

fn rotate_generic(path: &Path, direction: RotationDirection) -> Result<()> {
    let image = ImageReader::open(path)
        .with_context(|| format!("could not open {}", path.display()))?
        .with_guessed_format()
        .with_context(|| format!("could not detect image format for {}", path.display()))?
        .decode()
        .with_context(|| format!("could not decode {}", path.display()))?;
    let rotated = match direction {
        RotationDirection::Left => image.rotate270(),
        RotationDirection::Right => image.rotate90(),
    };

    let temp_path = temp_path_for(path)?;
    rotated
        .save(&temp_path)
        .with_context(|| format!("could not write {}", temp_path.display()))?;
    copy_temp_over_original(&temp_path, path)
}

fn write_bytes_via_temp(path: &Path, bytes: &[u8]) -> Result<()> {
    let temp_path = temp_path_for(path)?;
    fs::write(&temp_path, bytes)
        .with_context(|| format!("could not write {}", temp_path.display()))?;
    copy_temp_over_original(&temp_path, path)
}

fn copy_temp_over_original(temp_path: &Path, path: &Path) -> Result<()> {
    fs::copy(temp_path, path).with_context(|| format!("could not replace {}", path.display()))?;
    let _ = fs::remove_file(temp_path);
    Ok(())
}

fn temp_path_for(path: &Path) -> Result<PathBuf> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("image path has no file name")?;
    let temp_name = format!(".picturious-rotate-{}-{file_name}", std::process::id());
    Ok(path.with_file_name(temp_name))
}

fn is_jpeg(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            extension.eq_ignore_ascii_case("jpg") || extension.eq_ignore_ascii_case("jpeg")
        })
        .unwrap_or(false)
}

fn is_png(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.eq_ignore_ascii_case("png"))
        .unwrap_or(false)
}

fn rgba_over_white_to_rgb(rgba: image::RgbaImage) -> RgbImage {
    RgbImage::from_fn(rgba.width(), rgba.height(), |x, y| {
        let [red, green, blue, alpha] = rgba.get_pixel(x, y).0;
        Rgb([
            blend_channel_over_white(red, alpha),
            blend_channel_over_white(green, alpha),
            blend_channel_over_white(blue, alpha),
        ])
    })
}

fn blend_channel_over_white(channel: u8, alpha: u8) -> u8 {
    let channel = u16::from(channel);
    let alpha = u16::from(alpha);
    ((channel * alpha + 255 * (255 - alpha) + 127) / 255) as u8
}

fn clamp_jpeg_quality(quality: u8) -> u8 {
    quality.clamp(1, 100)
}

fn unique_jpg_path_for(path: &Path) -> Result<PathBuf> {
    let stem = path
        .file_stem()
        .and_then(|name| name.to_str())
        .context("image path has no file name")?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let first_candidate = parent.join(format!("{stem}.jpg"));
    if !first_candidate.exists() {
        return Ok(first_candidate);
    }

    for suffix in 1..=9999 {
        let candidate = parent.join(format!("{stem}-{suffix}.jpg"));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }

    bail!("could not choose an unused JPG path for {}", path.display())
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{Rgba, RgbaImage};
    use uuid::Uuid;

    #[test]
    fn convert_png_to_jpg_preserves_original_and_uses_unique_name() -> Result<()> {
        let folder =
            std::env::temp_dir().join(format!("picturious-convert-png-to-jpg-{}", Uuid::new_v4()));
        let _ = fs::remove_dir_all(&folder);
        fs::create_dir_all(&folder)?;
        let source = folder.join("alpha.png");
        let existing = folder.join("alpha.jpg");

        let mut png = RgbaImage::new(2, 1);
        png.put_pixel(0, 0, Rgba([255, 0, 0, 255]));
        png.put_pixel(1, 0, Rgba([0, 0, 255, 128]));
        png.save(&source)?;
        fs::write(&existing, b"existing jpg")?;

        let converted = convert_png_to_jpg(&source, 87)?;
        assert_eq!(
            converted.file_name().and_then(|name| name.to_str()),
            Some("alpha-1.jpg")
        );
        assert!(source.is_file());
        assert!(converted.is_file());
        assert_eq!(image_dimensions(&converted)?, (2, 1));

        let _ = fs::remove_dir_all(&folder);
        Ok(())
    }
}
