use image::{AnimationDecoder, DynamicImage, ImageFormat, RgbImage, RgbaImage};
use rust_processor::compression::{
    CompressionOptions, MAX_INPUT_BYTES, compress_image, compress_image_with_options,
};
use rust_processor::errors::CompressionError;
use std::io::Cursor;

fn png(image: &DynamicImage) -> Vec<u8> {
    let mut bytes = Cursor::new(Vec::new());
    image.write_to(&mut bytes, ImageFormat::Png).unwrap();
    bytes.into_inner()
}

#[test]
fn opaque_png_becomes_decodable_jpeg_with_original_dimensions() {
    let input = DynamicImage::ImageRgb8(RgbImage::from_pixel(12, 10, image::Rgb([50, 100, 150])));
    let output = compress_image(&png(&input)).unwrap();
    assert_eq!(image::guess_format(&output).unwrap(), ImageFormat::Jpeg);
    let decoded = image::load_from_memory(&output).unwrap();
    assert_eq!((decoded.width(), decoded.height()), (12, 10));
}

#[test]
fn transparent_png_becomes_webp_and_preserves_alpha() {
    let input = DynamicImage::ImageRgba8(RgbaImage::from_pixel(
        12,
        10,
        image::Rgba([50, 100, 150, 80]),
    ));
    let output = compress_image(&png(&input)).unwrap();
    assert_eq!(image::guess_format(&output).unwrap(), ImageFormat::WebP);
    let decoded = image::load_from_memory(&output).unwrap().to_rgba8();
    assert_eq!(decoded.dimensions(), (12, 10));
    assert_eq!(decoded.get_pixel(5, 5).0[3], 80);
}

#[test]
fn gif_is_passed_through_without_reencoding() {
    let image = DynamicImage::ImageRgba8(RgbaImage::from_pixel(2, 2, image::Rgba([0, 0, 0, 255])));
    let mut input = Cursor::new(Vec::new());
    image.write_to(&mut input, ImageFormat::Gif).unwrap();
    let input = input.into_inner();
    assert_eq!(compress_image(&input).unwrap(), input);
}

#[test]
fn invalid_image_returns_an_error() {
    assert!(matches!(
        compress_image(b"not an image"),
        Err(CompressionError::DecodeFailed(_))
    ));
}

fn textured() -> DynamicImage {
    DynamicImage::ImageRgb8(RgbImage::from_fn(256, 192, |x, y| {
        let n = (x.wrapping_mul(1664525) ^ y.wrapping_mul(1013904223)).wrapping_add(x * y);
        image::Rgb([n as u8, (n >> 8) as u8, (n >> 16) as u8])
    }))
}

#[test]
fn selected_quality_changes_the_encoded_size_and_output_stays_decodable() {
    let bytes = png(&textured());
    let encode = |quality| {
        compress_image_with_options(
            &bytes,
            CompressionOptions {
                quality,
                keep_smaller: false,
                ..Default::default()
            },
        )
        .unwrap()
    };
    let low = encode(40);
    let high = encode(95);
    assert!(low.data.len() < high.data.len());
    for output in [low, high] {
        let image = image::load_from_memory(&output.data).unwrap();
        assert_eq!((image.width(), image.height()), (256, 192));
        assert_eq!(output.mime(), "image/jpeg");
        assert_eq!(output.extension(), "jpg");
    }
}

#[test]
fn already_small_image_is_kept_byte_for_byte() {
    let input = png(&DynamicImage::ImageRgb8(RgbImage::from_pixel(
        1,
        1,
        image::Rgb([0, 0, 0]),
    )));
    let output = compress_image_with_options(&input, CompressionOptions::default()).unwrap();
    assert_eq!(output.data, input);
    assert!(!output.changed);
    assert_eq!(output.reason, "original_smaller");
    assert_eq!(output.mime(), "image/png");
}

#[test]
fn jpeg_bmp_tiff_and_webp_are_supported_by_content_not_filename() {
    for format in [
        ImageFormat::Jpeg,
        ImageFormat::Bmp,
        ImageFormat::Tiff,
        ImageFormat::WebP,
    ] {
        let mut input = Cursor::new(Vec::new());
        textured().write_to(&mut input, format).unwrap();
        let result = compress_image_with_options(
            input.get_ref(),
            CompressionOptions {
                keep_smaller: false,
                ..Default::default()
            },
        )
        .unwrap();
        let output = image::load_from_memory(&result.data).unwrap();
        assert_eq!((output.width(), output.height()), (256, 192));
        assert_eq!(image::guess_format(&result.data).unwrap(), result.format);
    }
}

#[test]
fn resizing_preserves_aspect_ratio_and_does_not_upscale() {
    let input = png(&textured());
    for (max_dimension, expected) in [(128, (128, 96)), (512, (256, 192))] {
        let output = compress_image_with_options(
            &input,
            CompressionOptions {
                max_dimension,
                keep_smaller: false,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!((output.width, output.height), expected);
        let decoded = image::load_from_memory(&output.data).unwrap();
        assert_eq!((decoded.width(), decoded.height()), expected);
    }
}

#[test]
fn exif_rotation_is_applied_before_metadata_is_discarded() {
    let mut input = Cursor::new(Vec::new());
    textured().write_to(&mut input, ImageFormat::Jpeg).unwrap();
    // Little-endian TIFF EXIF Orientation=6 (90 degrees clockwise).
    let exif = b"Exif\0\0II\x2a\0\x08\0\0\0\x01\0\x12\x01\x03\0\x01\0\0\0\x06\0\0\0\0\0\0\0";
    let mut bytes = vec![0xff, 0xd8, 0xff, 0xe1];
    bytes.extend_from_slice(&u16::try_from(exif.len() + 2).unwrap().to_be_bytes());
    bytes.extend_from_slice(exif);
    bytes.extend_from_slice(&input.get_ref()[2..]);
    let output = compress_image_with_options(
        &bytes,
        CompressionOptions {
            keep_smaller: false,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!((output.width, output.height), (192, 256));
    assert!(!output.data.windows(6).any(|b| b == b"Exif\0\0"));
    let decoded = image::load_from_memory(&output.data).unwrap();
    assert_eq!((decoded.width(), decoded.height()), (192, 256));
}

#[test]
fn animated_gif_and_apng_keep_all_frames_and_original_bytes() {
    let frame = RgbaImage::from_pixel(2, 2, image::Rgba([100, 120, 150, 80]));
    let mut gif = Vec::new();
    image::codecs::gif::GifEncoder::new(&mut gif)
        .encode_frames(vec![
            image::Frame::new(frame.clone()),
            image::Frame::new(frame),
        ])
        .unwrap();
    assert_eq!(
        image::codecs::gif::GifDecoder::new(Cursor::new(&gif))
            .unwrap()
            .into_frames()
            .count(),
        2
    );
    let mut apng = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut apng, 2, 2);
        encoder.set_color(png::ColorType::Rgba);
        encoder.set_depth(png::BitDepth::Eight);
        encoder.set_animated(2, 0).unwrap();
        let mut writer = encoder.write_header().unwrap();
        writer
            .write_image_data(&[100, 120, 150, 80].repeat(4))
            .unwrap();
        writer
            .write_image_data(&[20, 40, 60, 255].repeat(4))
            .unwrap();
        writer.finish().unwrap();
    }
    for input in [gif, apng] {
        let output = compress_image_with_options(&input, CompressionOptions::default()).unwrap();
        assert_eq!(output.data, input);
        assert_eq!(output.reason, "animation");
        assert!(!output.changed);
    }
}

#[test]
fn multiple_tiff_pages_are_not_silently_flattened() {
    let mut input = Cursor::new(Vec::new());
    {
        let mut writer = tiff::encoder::TiffEncoder::new(&mut input).unwrap();
        writer
            .write_image::<tiff::encoder::colortype::RGB8>(2, 2, &[40, 80, 120].repeat(4))
            .unwrap();
        writer
            .write_image::<tiff::encoder::colortype::RGB8>(2, 2, &[120, 80, 40].repeat(4))
            .unwrap();
    }
    let output =
        compress_image_with_options(input.get_ref(), CompressionOptions::default()).unwrap();
    assert_eq!(output.data, *input.get_ref());
    assert_eq!(output.reason, "multipage");
}

#[test]
fn invalid_options_and_oversized_inputs_are_rejected() {
    let input = png(&textured());
    for quality in [0, 101, 255] {
        assert!(matches!(
            compress_image_with_options(
                &input,
                CompressionOptions {
                    quality,
                    ..Default::default()
                }
            ),
            Err(CompressionError::InvalidQuality(_))
        ));
    }
    assert!(matches!(
        compress_image_with_options(
            &input,
            CompressionOptions {
                max_dimension: 1,
                ..Default::default()
            }
        ),
        Err(CompressionError::InvalidMaxDimension(_))
    ));
    assert!(matches!(
        compress_image(&vec![0; MAX_INPUT_BYTES + 1]),
        Err(CompressionError::InputTooLarge)
    ));
    assert!(compress_image(b"GIF89a").is_err());
}

#[test]
fn excessive_dimensions_are_rejected_before_allocating_pixels() {
    let mut input = png(&DynamicImage::ImageRgb8(RgbImage::new(1, 1)));
    input[16..20].copy_from_slice(&20_000u32.to_be_bytes());
    // Update IHDR CRC so dimension validation, rather than corrupt data, rejects it.
    let mut crc = !0u32;
    for byte in &input[12..29] {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb88320u32 & 0u32.wrapping_sub(crc & 1));
        }
    }
    input[29..33].copy_from_slice(&(!crc).to_be_bytes());
    assert!(matches!(
        compress_image(&input),
        Err(CompressionError::ImageTooLarge)
    ));
}
