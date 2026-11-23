//! Privacy preparation, separate from optional compression's original fallback.
//! Sync native foundation; the desktop caller must own its bounded worker slot.
use super::codec::{self, CompressedImage, MAX_INPUT_BYTES, MAX_PIXELS};
use crate::errors::CompressionError;
use image::{DynamicImage, ImageDecoder, ImageEncoder, ImageFormat, ImageReader};
use std::io::{Cursor, Write};

#[derive(Clone, Copy)]
pub enum PhotoChoice {
    /// None means fresh lossless PNG pixels; Some selects JPEG/WebP quality.
    Clean {
        quality: Option<u8>,
        max_dimension: u32,
    },
    /// A separate explicit choice; never inferred from compression size/failure.
    Original { metadata_warning_acknowledged: bool },
}
impl Default for PhotoChoice {
    fn default() -> Self {
        Self::Clean {
            quality: None,
            max_dimension: 0,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PhotoPreparationError {
    #[error(transparent)]
    Codec(#[from] CompressionError),
    #[error("sending original photo bytes requires acknowledgement of retained metadata")]
    OriginalNeedsConfirmation,
    #[error("animation requires an explicit original choice; automatic frame loss is forbidden")]
    AnimationNeedsOriginal,
    #[error("multipage TIFF requires an explicit original choice")]
    MultipageNeedsOriginal,
    #[error(
        "prepared photo exceeds the output size limit; choose compression or smaller dimensions"
    )]
    OutputTooLarge,
}

/// No caller-set metadata flag, Debug/Clone/serde or file path. Only this module
/// can mint the assertion that source ancillary metadata was discarded.
pub struct PreparedPhoto {
    image: CompressedImage,
    stripped: bool,
}
impl PreparedPhoto {
    pub fn as_bytes(&self) -> &[u8] {
        &self.image.data
    }
    pub fn into_bytes(self) -> Vec<u8> {
        self.image.data
    }
    pub fn mime(&self) -> &'static str {
        self.image.mime()
    }
    pub fn extension(&self) -> &'static str {
        self.image.extension()
    }
    pub fn dimensions(&self) -> (u32, u32) {
        (self.image.width, self.image.height)
    }
    pub fn metadata_stripped(&self) -> bool {
        self.stripped
    }
}

pub fn prepare_photo(
    data: &[u8],
    choice: PhotoChoice,
) -> Result<PreparedPhoto, PhotoPreparationError> {
    prepare_with_limit(data, choice, MAX_INPUT_BYTES)
}

fn prepare_with_limit(
    data: &[u8],
    choice: PhotoChoice,
    output_limit: usize,
) -> Result<PreparedPhoto, PhotoPreparationError> {
    if data.len() > MAX_INPUT_BYTES {
        return Err(CompressionError::InputTooLarge.into());
    }
    if matches!(
        choice,
        PhotoChoice::Original {
            metadata_warning_acknowledged: false
        }
    ) {
        return Err(PhotoPreparationError::OriginalNeedsConfirmation);
    }
    if let PhotoChoice::Clean {
        quality,
        max_dimension,
    } = choice
    {
        if let Some(q) = quality
            && !(1..=100).contains(&q)
        {
            return Err(CompressionError::InvalidQuality(q).into());
        }
        if max_dimension != 0 && !(64..=codec::MAX_SIDE).contains(&max_dimension) {
            return Err(CompressionError::InvalidMaxDimension(max_dimension).into());
        }
    }
    let format = image::guess_format(data).map_err(codec::decode_error)?;
    if !matches!(
        format,
        ImageFormat::Jpeg
            | ImageFormat::Png
            | ImageFormat::WebP
            | ImageFormat::Gif
            | ImageFormat::Bmp
            | ImageFormat::Tiff
    ) {
        return Err(CompressionError::UnsupportedFormat.into());
    }
    let mut reader = ImageReader::with_format(Cursor::new(data), format);
    reader.limits(codec::limits());
    let mut decoder = reader.into_decoder().map_err(codec::decode_error)?;
    let (width, height) = decoder.dimensions();
    if width > codec::MAX_SIDE
        || height > codec::MAX_SIDE
        || u64::from(width) * u64::from(height) > MAX_PIXELS
        || decoder.total_bytes() > codec::MAX_DECODED_BYTES
    {
        return Err(CompressionError::ImageTooLarge.into());
    }
    let orientation = decoder.orientation().map_err(codec::decode_error)?;
    if matches!(choice, PhotoChoice::Original { .. }) {
        if data.len() > output_limit {
            return Err(PhotoPreparationError::OutputTooLarge);
        }
        let (width, height) = if orientation.to_exif() >= 5 {
            (height, width)
        } else {
            (width, height)
        };
        return Ok(PreparedPhoto {
            image: CompressedImage {
                data: data.to_vec(),
                format,
                width,
                height,
                changed: false,
                reason: "explicit_original",
            },
            stripped: false,
        });
    }
    let animated = match format {
        ImageFormat::Gif => true,
        ImageFormat::Png => {
            image::codecs::png::PngDecoder::with_limits(Cursor::new(data), codec::limits())
                .map_err(codec::decode_error)?
                .is_apng()
                .map_err(codec::decode_error)?
        }
        ImageFormat::WebP => image::codecs::webp::WebPDecoder::new(Cursor::new(data))
            .map_err(codec::decode_error)?
            .has_animation(),
        _ => false,
    };
    if animated {
        return Err(PhotoPreparationError::AnimationNeedsOriginal);
    }
    if format == ImageFormat::Tiff && codec::multipage_tiff(data) {
        return Err(PhotoPreparationError::MultipageNeedsOriginal);
    }
    let mut pixels = DynamicImage::from_decoder(decoder).map_err(codec::decode_error)?;
    pixels.apply_orientation(orientation);
    let PhotoChoice::Clean {
        quality,
        max_dimension,
    } = choice
    else {
        unreachable!()
    };
    if max_dimension != 0 && (pixels.width() > max_dimension || pixels.height() > max_dimension) {
        pixels = pixels.resize(
            max_dimension,
            max_dimension,
            image::imageops::FilterType::Triangle,
        );
    }
    let (bytes, format) = if let Some(quality) = quality {
        codec::encode_pixels(&pixels, quality)?
    } else {
        // Fresh pixels only: no copied EXIF/XMP/ICC/text/trailing source chunks.
        let mut sink = Capped {
            bytes: Vec::new(),
            limit: output_limit,
            overflow: false,
        };
        let result = image::codecs::png::PngEncoder::new(&mut sink).write_image(
            pixels.as_bytes(),
            pixels.width(),
            pixels.height(),
            pixels.color().into(),
        );
        if sink.overflow {
            return Err(PhotoPreparationError::OutputTooLarge);
        }
        result.map_err(|error| CompressionError::EncodeFailed(error.to_string()))?;
        (sink.bytes, ImageFormat::Png)
    };
    if bytes.len() > output_limit {
        return Err(PhotoPreparationError::OutputTooLarge);
    }
    Ok(PreparedPhoto {
        image: CompressedImage {
            data: bytes,
            format,
            width: pixels.width(),
            height: pixels.height(),
            changed: true,
            reason: "metadata_stripped",
        },
        stripped: true,
    })
}

struct Capped {
    bytes: Vec<u8>,
    limit: usize,
    overflow: bool,
}
impl Write for Capped {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.overflow = true;
            return Err(std::io::Error::other("photo output limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "photo_tests.rs"]
mod tests;
