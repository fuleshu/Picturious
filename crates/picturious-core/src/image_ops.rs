use anyhow::{Context, Result};
use image::ImageReader;
use std::fs;
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
