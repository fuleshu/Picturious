use anyhow::{Context, Result};
use image::{DynamicImage, RgbImage};
use std::path::Path;

pub fn is_heic_path(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            ["heic", "heif", "hif"]
                .iter()
                .any(|supported| extension.eq_ignore_ascii_case(supported))
        })
        .unwrap_or(false)
}

pub fn decode_heic_image(path: &Path, max_dimension: Option<u32>) -> Result<DynamicImage> {
    let decoded = platform::decode_heic_rgb8(path, max_dimension)?;
    let image = RgbImage::from_raw(decoded.width, decoded.height, decoded.rgb)
        .context("HEIC decoder produced an invalid RGB buffer")?;
    Ok(DynamicImage::ImageRgb8(image))
}

pub fn heic_dimensions(path: &Path) -> Result<(u32, u32)> {
    platform::heic_dimensions(path)
}

#[cfg(windows)]
mod platform {
    use super::*;
    use std::ffi::OsStr;
    use std::os::windows::ffi::OsStrExt;
    use windows::Win32::Foundation::{GENERIC_READ, RPC_E_CHANGED_MODE, S_FALSE, S_OK};
    use windows::Win32::Graphics::Imaging::{
        CLSID_WICImagingFactory, GUID_WICPixelFormat24bppBGR, GUID_WICPixelFormat24bppRGB,
        GUID_WICPixelFormat32bppBGR, GUID_WICPixelFormat32bppBGRA,
        GUID_WICPixelFormat32bppRGBA1010102, GUID_WICPixelFormat32bppRGBA1010102XR,
        GUID_WICPixelFormat48bppRGB, GUID_WICPixelFormat48bppRGBFixedPoint,
        GUID_WICPixelFormat48bppRGBHalf, GUID_WICPixelFormat64bppRGBA,
        GUID_WICPixelFormat64bppRGBAFixedPoint, GUID_WICPixelFormat64bppRGBAHalf,
        GUID_WICPixelFormat96bppRGBFixedPoint, GUID_WICPixelFormat96bppRGBFloat,
        GUID_WICPixelFormat128bppRGBAFixedPoint, GUID_WICPixelFormat128bppRGBAFloat,
        GUID_WICPixelFormat128bppRGBFixedPoint, GUID_WICPixelFormat128bppRGBFloat,
        IWICBitmapSource, IWICBitmapSourceTransform, IWICImagingFactory, IWICPalette,
        WICBitmapDitherTypeNone, WICBitmapInterpolationModeFant, WICBitmapPaletteTypeCustom,
        WICBitmapTransformFlipHorizontal, WICBitmapTransformFlipVertical,
        WICBitmapTransformOptions, WICBitmapTransformRotate0, WICBitmapTransformRotate90,
        WICBitmapTransformRotate180, WICBitmapTransformRotate270, WICDecodeMetadataCacheOnLoad,
    };
    use windows::Win32::System::Com::StructuredStorage::{
        PROPVARIANT, PropVariantClear, PropVariantToUInt16,
    };
    use windows::Win32::System::Com::{
        CLSCTX_INPROC_SERVER, COINIT_MULTITHREADED, CoCreateInstance, CoInitializeEx,
        CoUninitialize,
    };
    use windows::core::{GUID, Interface, PCWSTR};

    pub struct DecodedHeicImage {
        pub width: u32,
        pub height: u32,
        pub rgb: Vec<u8>,
    }

    #[derive(Clone, Copy)]
    enum FastTransformLayout {
        Rgb,
        Bgr,
    }

    struct ComApartment {
        should_uninitialize: bool,
    }

    impl ComApartment {
        fn initialize() -> Result<Self> {
            let hr = unsafe { CoInitializeEx(None, COINIT_MULTITHREADED) };
            if hr == RPC_E_CHANGED_MODE {
                return Ok(Self {
                    should_uninitialize: false,
                });
            }

            hr.ok()
                .context("could not initialize COM for Windows HEIC decoding")?;
            Ok(Self {
                should_uninitialize: hr == S_OK || hr == S_FALSE,
            })
        }
    }

    impl Drop for ComApartment {
        fn drop(&mut self) {
            if self.should_uninitialize {
                unsafe { CoUninitialize() };
            }
        }
    }

    pub fn heic_dimensions(path: &Path) -> Result<(u32, u32)> {
        let _com = ComApartment::initialize()?;
        let factory = wic_factory()?;
        let decoder = wic_decoder_for_path(&factory, path)?;
        let frame = unsafe { decoder.GetFrame(0) }
            .with_context(|| format!("could not read first HEIC frame from {}", path.display()))?;
        let source = oriented_source_for_frame(&factory, &frame)?;
        source_size(&source)
    }

    pub fn decode_heic_rgb8(path: &Path, max_dimension: Option<u32>) -> Result<DecodedHeicImage> {
        let _com = ComApartment::initialize()?;
        let factory = wic_factory()?;
        let decoder = wic_decoder_for_path(&factory, path)?;
        let frame = unsafe { decoder.GetFrame(0) }
            .with_context(|| format!("could not read first HEIC frame from {}", path.display()))?;
        let source: IWICBitmapSource = frame.cast()?;
        let (source_width, source_height) = source_size(&source)?;
        let orientation = frame_orientation(&frame);
        let (oriented_width, oriented_height) =
            oriented_dimensions(source_width, source_height, orientation);
        let (target_width, target_height) =
            scaled_dimensions(oriented_width, oriented_height, max_dimension);
        let transform = orientation
            .and_then(orientation_transform)
            .unwrap_or(WICBitmapTransformRotate0);

        if let Some(decoded) =
            decode_with_source_transform(&frame, transform, target_width, target_height)?
        {
            return Ok(decoded);
        }

        let oriented_source =
            oriented_source_for_frame_with_orientation(&factory, &frame, orientation)?;
        let source_format = unsafe { oriented_source.GetPixelFormat() }
            .with_context(|| format!("could not read HEIC pixel format for {}", path.display()))?;
        let source: IWICBitmapSource =
            if target_width != oriented_width || target_height != oriented_height {
                let scaler = unsafe { factory.CreateBitmapScaler() }
                    .context("could not create Windows HEIC scaler")?;
                unsafe {
                    scaler.Initialize(
                        &oriented_source,
                        target_width,
                        target_height,
                        WICBitmapInterpolationModeFant,
                    )
                }
                .context("could not scale HEIC image")?;
                scaler.cast()?
            } else {
                oriented_source
            };

        let converter = unsafe { factory.CreateFormatConverter() }
            .context("could not create Windows HEIC color converter")?;
        unsafe {
            converter.Initialize(
                &source,
                &GUID_WICPixelFormat24bppRGB,
                WICBitmapDitherTypeNone,
                Option::<&IWICPalette>::None,
                0.0,
                WICBitmapPaletteTypeCustom,
            )
        }
        .with_context(|| {
            let detail = if is_high_bit_depth_or_float_format(&source_format) {
                "high-bit-depth HEIC source"
            } else {
                "HEIC source"
            };
            format!("could not convert {detail} to RGB8 for {}", path.display())
        })?;

        let stride = target_width
            .checked_mul(3)
            .context("HEIC image is too wide to copy")?;
        let byte_len = stride
            .checked_mul(target_height)
            .context("HEIC image is too large to copy")?;
        let mut rgb = vec![0; byte_len as usize];
        unsafe { converter.CopyPixels(std::ptr::null(), stride, &mut rgb) }
            .with_context(|| format!("could not copy HEIC pixels from {}", path.display()))?;

        Ok(DecodedHeicImage {
            width: target_width,
            height: target_height,
            rgb,
        })
    }

    fn decode_with_source_transform(
        frame: &windows::Win32::Graphics::Imaging::IWICBitmapFrameDecode,
        transform: WICBitmapTransformOptions,
        target_width: u32,
        target_height: u32,
    ) -> Result<Option<DecodedHeicImage>> {
        let Ok(source_transform) = frame.cast::<IWICBitmapSourceTransform>() else {
            return Ok(None);
        };
        let Ok(transform_supported) = (unsafe { source_transform.DoesSupportTransform(transform) })
        else {
            return Ok(None);
        };
        if !transform_supported.as_bool() {
            return Ok(None);
        }

        let mut width = target_width;
        let mut height = target_height;
        if unsafe { source_transform.GetClosestSize(&mut width, &mut height) }.is_err() {
            return Ok(None);
        }
        if width != target_width || height != target_height {
            return Ok(None);
        }

        for (pixel_format, bytes_per_pixel, layout) in [
            (GUID_WICPixelFormat24bppRGB, 3, FastTransformLayout::Rgb),
            (GUID_WICPixelFormat24bppBGR, 3, FastTransformLayout::Bgr),
            (GUID_WICPixelFormat32bppBGRA, 4, FastTransformLayout::Bgr),
            (GUID_WICPixelFormat32bppBGR, 4, FastTransformLayout::Bgr),
        ] {
            if let Some(decoded) = copy_source_transform_pixels(
                &source_transform,
                pixel_format,
                bytes_per_pixel,
                layout,
                transform,
                target_width,
                target_height,
            )? {
                return Ok(Some(decoded));
            }
        }

        Ok(None)
    }

    fn copy_source_transform_pixels(
        source_transform: &IWICBitmapSourceTransform,
        pixel_format: GUID,
        bytes_per_pixel: u32,
        layout: FastTransformLayout,
        transform: WICBitmapTransformOptions,
        target_width: u32,
        target_height: u32,
    ) -> Result<Option<DecodedHeicImage>> {
        let stride = target_width
            .checked_mul(bytes_per_pixel)
            .context("HEIC image is too wide to copy")?;
        let byte_len = stride
            .checked_mul(target_height)
            .context("HEIC image is too large to copy")?;
        let mut pixels = vec![0; byte_len as usize];
        if unsafe {
            source_transform.CopyPixels(
                std::ptr::null(),
                target_width,
                target_height,
                &pixel_format,
                transform,
                stride,
                &mut pixels,
            )
        }
        .is_err()
        {
            return Ok(None);
        }

        let rgb = pixels_to_rgb(pixels, bytes_per_pixel as usize, layout);
        Ok(Some(DecodedHeicImage {
            width: target_width,
            height: target_height,
            rgb,
        }))
    }

    fn pixels_to_rgb(
        pixels: Vec<u8>,
        bytes_per_pixel: usize,
        layout: FastTransformLayout,
    ) -> Vec<u8> {
        match layout {
            FastTransformLayout::Rgb if bytes_per_pixel == 3 => pixels,
            FastTransformLayout::Rgb => pixels
                .chunks_exact(bytes_per_pixel)
                .flat_map(|pixel| [pixel[0], pixel[1], pixel[2]])
                .collect(),
            FastTransformLayout::Bgr => pixels
                .chunks_exact(bytes_per_pixel)
                .flat_map(|pixel| [pixel[2], pixel[1], pixel[0]])
                .collect(),
        }
    }

    fn wic_factory() -> Result<IWICImagingFactory> {
        unsafe {
            CoCreateInstance(&CLSID_WICImagingFactory, None, CLSCTX_INPROC_SERVER)
                .context("could not create Windows Imaging Component factory")
        }
    }

    fn wic_decoder_for_path(
        factory: &IWICImagingFactory,
        path: &Path,
    ) -> Result<windows::Win32::Graphics::Imaging::IWICBitmapDecoder> {
        let wide_path = wide_null(path.as_os_str());
        unsafe {
            factory.CreateDecoderFromFilename(
                PCWSTR(wide_path.as_ptr()),
                None,
                GENERIC_READ,
                WICDecodeMetadataCacheOnLoad,
            )
        }
        .with_context(|| {
            format!(
                "could not decode HEIC image {}; install Windows HEIF/HEVC image extensions if needed",
                path.display()
            )
        })
    }

    fn source_size(source: &IWICBitmapSource) -> Result<(u32, u32)> {
        let mut width = 0_u32;
        let mut height = 0_u32;
        unsafe { source.GetSize(&mut width, &mut height) }.context("could not read image size")?;
        Ok((width, height))
    }

    fn oriented_source_for_frame(
        factory: &IWICImagingFactory,
        frame: &windows::Win32::Graphics::Imaging::IWICBitmapFrameDecode,
    ) -> Result<IWICBitmapSource> {
        oriented_source_for_frame_with_orientation(factory, frame, frame_orientation(frame))
    }

    fn oriented_source_for_frame_with_orientation(
        factory: &IWICImagingFactory,
        frame: &windows::Win32::Graphics::Imaging::IWICBitmapFrameDecode,
        orientation: Option<u16>,
    ) -> Result<IWICBitmapSource> {
        let Some(transform) = orientation.and_then(orientation_transform) else {
            return Ok(frame.cast()?);
        };

        let rotator = unsafe { factory.CreateBitmapFlipRotator() }
            .context("could not create Windows HEIC orientation transform")?;
        unsafe { rotator.Initialize(frame, transform) }
            .context("could not apply HEIC orientation")?;
        Ok(rotator.cast()?)
    }

    fn oriented_dimensions(width: u32, height: u32, orientation: Option<u16>) -> (u32, u32) {
        match orientation {
            Some(5 | 6 | 7 | 8) => (height, width),
            _ => (width, height),
        }
    }

    fn frame_orientation(
        frame: &windows::Win32::Graphics::Imaging::IWICBitmapFrameDecode,
    ) -> Option<u16> {
        let reader = unsafe { frame.GetMetadataQueryReader().ok()? };
        for query in [
            "/ifd/{ushort=274}",
            "/app1/ifd/{ushort=274}",
            "/ifd/exif/{ushort=274}",
            "/exif/ifd/{ushort=274}",
        ] {
            let wide_query = wide_null(OsStr::new(query));
            let mut value = PROPVARIANT::default();
            let read = unsafe { reader.GetMetadataByName(PCWSTR(wide_query.as_ptr()), &mut value) };
            if read.is_err() {
                continue;
            }

            let orientation = unsafe { PropVariantToUInt16(&value).ok() };
            let _ = unsafe { PropVariantClear(&mut value) };
            if let Some(orientation @ 1..=8) = orientation {
                return Some(orientation);
            }
        }

        None
    }

    fn orientation_transform(orientation: u16) -> Option<WICBitmapTransformOptions> {
        match orientation {
            1 => None,
            2 => Some(WICBitmapTransformFlipHorizontal),
            3 => Some(WICBitmapTransformRotate180),
            4 => Some(WICBitmapTransformFlipVertical),
            5 => Some(combine_transform(
                WICBitmapTransformRotate90,
                WICBitmapTransformFlipHorizontal,
            )),
            6 => Some(WICBitmapTransformRotate90),
            7 => Some(combine_transform(
                WICBitmapTransformRotate270,
                WICBitmapTransformFlipHorizontal,
            )),
            8 => Some(WICBitmapTransformRotate270),
            _ => None,
        }
    }

    fn combine_transform(
        left: WICBitmapTransformOptions,
        right: WICBitmapTransformOptions,
    ) -> WICBitmapTransformOptions {
        WICBitmapTransformOptions(left.0 | right.0)
    }

    fn scaled_dimensions(width: u32, height: u32, max_dimension: Option<u32>) -> (u32, u32) {
        let Some(max_dimension) = max_dimension.filter(|size| *size > 0) else {
            return (width, height);
        };
        let longest = width.max(height);
        if longest <= max_dimension {
            return (width, height);
        }

        let scale = max_dimension as f64 / longest as f64;
        (
            ((width as f64 * scale).round() as u32).max(1),
            ((height as f64 * scale).round() as u32).max(1),
        )
    }

    fn wide_null(value: &OsStr) -> Vec<u16> {
        value.encode_wide().chain(std::iter::once(0)).collect()
    }

    fn is_high_bit_depth_or_float_format(format: &GUID) -> bool {
        [
            GUID_WICPixelFormat32bppRGBA1010102,
            GUID_WICPixelFormat32bppRGBA1010102XR,
            GUID_WICPixelFormat48bppRGB,
            GUID_WICPixelFormat48bppRGBFixedPoint,
            GUID_WICPixelFormat48bppRGBHalf,
            GUID_WICPixelFormat64bppRGBA,
            GUID_WICPixelFormat64bppRGBAFixedPoint,
            GUID_WICPixelFormat64bppRGBAHalf,
            GUID_WICPixelFormat96bppRGBFixedPoint,
            GUID_WICPixelFormat96bppRGBFloat,
            GUID_WICPixelFormat128bppRGBFixedPoint,
            GUID_WICPixelFormat128bppRGBFloat,
            GUID_WICPixelFormat128bppRGBAFixedPoint,
            GUID_WICPixelFormat128bppRGBAFloat,
        ]
        .iter()
        .any(|candidate| candidate == format)
    }
}

#[cfg(not(windows))]
mod platform {
    use super::*;
    use anyhow::bail;

    pub struct DecodedHeicImage {
        pub width: u32,
        pub height: u32,
        pub rgb: Vec<u8>,
    }

    pub fn heic_dimensions(path: &Path) -> Result<(u32, u32)> {
        bail!(
            "HEIC decoding is currently implemented through Windows Imaging Component: {}",
            path.display()
        )
    }

    pub fn decode_heic_rgb8(path: &Path, _max_dimension: Option<u32>) -> Result<DecodedHeicImage> {
        bail!(
            "HEIC decoding is currently implemented through Windows Imaging Component: {}",
            path.display()
        )
    }
}
