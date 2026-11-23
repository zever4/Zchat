use crate::errors::CompressionError;
use image::{DynamicImage, ImageDecoder, ImageError, ImageFormat, ImageReader};
use std::io::Cursor;

pub const MAX_INPUT_BYTES: usize = 20 * 1024 * 1024;
pub const MAX_PIXELS: u64 = 24_000_000;
pub(super) const MAX_SIDE: u32 = 12_000;
pub(super) const MAX_DECODED_BYTES: u64 = 96 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct CompressionOptions {
    pub quality: u8,
    /// Zero keeps original dimensions; a nonzero value never upscales.
    pub max_dimension: u32,
    pub keep_smaller: bool,
}

impl Default for CompressionOptions {
    fn default() -> Self {
        Self {
            quality: 85,
            max_dimension: 0,
            keep_smaller: true,
        }
    }
}

pub struct CompressedImage {
    pub data: Vec<u8>,
    pub format: ImageFormat,
    pub width: u32,
    pub height: u32,
    pub changed: bool,
    pub reason: &'static str,
}

impl CompressedImage {
    pub fn mime(&self) -> &'static str {
        match self.format {
            ImageFormat::Jpeg => "image/jpeg",
            ImageFormat::Png => "image/png",
            ImageFormat::WebP => "image/webp",
            ImageFormat::Gif => "image/gif",
            ImageFormat::Bmp => "image/bmp",
            ImageFormat::Tiff => "image/tiff",
            _ => unreachable!("only supported image formats are retained"),
        }
    }

    pub fn extension(&self) -> &'static str {
        match self.format {
            ImageFormat::Jpeg => "jpg",
            ImageFormat::Png => "png",
            ImageFormat::WebP => "webp",
            ImageFormat::Gif => "gif",
            ImageFormat::Bmp => "bmp",
            ImageFormat::Tiff => "tiff",
            _ => unreachable!("only supported image formats are retained"),
        }
    }
}

/// Compatibility API: encode with historical default quality. Desktop uses the
/// richer API, retaining an original when re-encoding would make it larger.
pub fn compress_image(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    compress_image_with_options(
        data,
        CompressionOptions {
            keep_smaller: false,
            ..Default::default()
        },
    )
    .map(|result| result.data)
}

pub fn compress_image_with_options(
    data: &[u8],
    options: CompressionOptions,
) -> Result<CompressedImage, CompressionError> {
    if data.len() > MAX_INPUT_BYTES {
        return Err(CompressionError::InputTooLarge);
    }
    if !(1..=100).contains(&options.quality) {
        return Err(CompressionError::InvalidQuality(options.quality));
    }
    if options.max_dimension != 0 && !(64..=MAX_SIDE).contains(&options.max_dimension) {
        return Err(CompressionError::InvalidMaxDimension(options.max_dimension));
    }
    let format = image::guess_format(data).map_err(decode_error)?;
    if !matches!(
        format,
        ImageFormat::Jpeg
            | ImageFormat::Png
            | ImageFormat::WebP
            | ImageFormat::Gif
            | ImageFormat::Bmp
            | ImageFormat::Tiff
    ) {
        return Err(CompressionError::UnsupportedFormat);
    }
    // Do not flatten animation or multiple TIFF pages to a single frame.
    let animated = match format {
        ImageFormat::Gif => true,
        ImageFormat::Png => {
            image::codecs::png::PngDecoder::with_limits(Cursor::new(data), limits())
                .map_err(decode_error)?
                .is_apng()
                .map_err(decode_error)?
        }
        ImageFormat::WebP => image::codecs::webp::WebPDecoder::new(Cursor::new(data))
            .map_err(decode_error)?
            .has_animation(),
        _ => false,
    };
    let mut reader = ImageReader::with_format(Cursor::new(data), format);
    reader.limits(limits());
    let mut decoder = reader.into_decoder().map_err(decode_error)?;
    let (width, height) = decoder.dimensions();
    if u64::from(width) * u64::from(height) > MAX_PIXELS
        || decoder.total_bytes() > MAX_DECODED_BYTES
    {
        return Err(CompressionError::ImageTooLarge);
    }
    if animated || (format == ImageFormat::Tiff && multipage_tiff(data)) {
        return Ok(CompressedImage {
            data: data.to_vec(),
            format,
            width,
            height,
            changed: false,
            reason: if animated { "animation" } else { "multipage" },
        });
    }
    let orientation = decoder.orientation().map_err(decode_error)?;
    let mut image = DynamicImage::from_decoder(decoder).map_err(decode_error)?;
    image.apply_orientation(orientation);
    if options.max_dimension != 0
        && (image.width() > options.max_dimension || image.height() > options.max_dimension)
    {
        image = image.resize(
            options.max_dimension,
            options.max_dimension,
            image::imageops::FilterType::Triangle,
        );
    }
    let (encoded, output_format) = encode_pixels(&image, options.quality)?;
    // A larger result never replaces the original. The UI sends BMP/TIFF
    // originals as files, since those formats are not inline chat media.
    if options.keep_smaller && encoded.len() >= data.len() {
        let (width, height) = if orientation.to_exif() >= 5 {
            (height, width)
        } else {
            (width, height)
        };
        return Ok(CompressedImage {
            data: data.to_vec(),
            format,
            width,
            height,
            changed: false,
            reason: "original_smaller",
        });
    }
    Ok(CompressedImage {
        data: encoded,
        format: output_format,
        width: image.width(),
        height: image.height(),
        changed: true,
        reason: "compressed",
    })
}

pub(super) fn limits() -> image::Limits {
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(MAX_SIDE);
    limits.max_image_height = Some(MAX_SIDE);
    limits.max_alloc = Some(MAX_DECODED_BYTES);
    limits
}

pub(super) fn decode_error(error: ImageError) -> CompressionError {
    match error {
        ImageError::Limits(_) => CompressionError::ImageTooLarge,
        _ => CompressionError::DecodeFailed(error.to_string()),
    }
}

// Inspect the first IFD's next pointer, with checked offsets for classic TIFF
// and BigTIFF. Full image validation is left to the decoder.
pub(super) fn multipage_tiff(data: &[u8]) -> bool {
    let little = data.starts_with(b"II");
    let integer = |offset: usize, size: usize| -> Option<u64> {
        let bytes = data.get(offset..offset.checked_add(size)?)?;
        Some(if little {
            bytes.iter().rev().fold(0, |v, b| (v << 8) | u64::from(*b))
        } else {
            bytes.iter().fold(0, |v, b| (v << 8) | u64::from(*b))
        })
    };
    let (offset_pos, pointer_size, count_size, entry_size) = if integer(2, 2) == Some(43) {
        (8, 8, 8, 20u64)
    } else {
        (4, 4, 2, 12u64)
    };
    let Some(ifd) = integer(offset_pos, pointer_size).and_then(|n| usize::try_from(n).ok()) else {
        return false;
    };
    let next = integer(ifd, count_size)
        .and_then(|n| n.checked_mul(entry_size))
        .and_then(|n| n.checked_add(count_size as u64))
        .and_then(|n| usize::try_from(n).ok())
        .and_then(|n| ifd.checked_add(n));
    next.and_then(|offset| integer(offset, pointer_size))
        .is_some_and(|offset| offset != 0)
}

fn compress_jpeg(image: &DynamicImage, quality: u8) -> Result<Vec<u8>, CompressionError> {
    let rgb = image.to_rgb8();
    let mut compressor = mozjpeg::Compress::new(mozjpeg::ColorSpace::JCS_RGB);
    compressor.set_quality(f32::from(quality));
    compressor.set_size(rgb.width() as usize, rgb.height() as usize);
    let mut output = Vec::new();
    let mut started = compressor
        .start_compress(&mut output)
        .map_err(|e| CompressionError::EncodeFailed(e.to_string()))?;
    started
        .write_scanlines(rgb.as_raw())
        .map_err(|e| CompressionError::EncodeFailed(e.to_string()))?;
    started
        .finish()
        .map_err(|e| CompressionError::EncodeFailed(e.to_string()))?;
    Ok(output)
}

pub(super) fn encode_pixels(
    image: &DynamicImage,
    quality: u8,
) -> Result<(Vec<u8>, ImageFormat), CompressionError> {
    if image.color().has_alpha() {
        let rgba = image.to_rgba8();
        Ok((
            webp::Encoder::from_rgba(rgba.as_raw(), rgba.width(), rgba.height())
                .encode_simple(false, f32::from(quality))
                .map_err(|error| CompressionError::EncodeFailed(format!("{error:?}")))?
                .to_vec(),
            ImageFormat::WebP,
        ))
    } else {
        Ok((compress_jpeg(image, quality)?, ImageFormat::Jpeg))
    }
}
