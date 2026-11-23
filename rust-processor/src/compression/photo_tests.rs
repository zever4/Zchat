use super::*;
use image::{RgbImage, RgbaImage};

fn encode(image: &DynamicImage, format: ImageFormat) -> Vec<u8> {
    let mut bytes = Cursor::new(Vec::new());
    image.write_to(&mut bytes, format).unwrap();
    bytes.into_inner()
}
fn tagged_png() -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut bytes, 1, 1);
        encoder.set_color(png::ColorType::Rgb);
        encoder
            .add_text_chunk(
                "Comment".into(),
                "PRIVATE-GPS-55.45-37.37-CAMERA-SERIAL".into(),
            )
            .unwrap();
        let mut writer = encoder.write_header().unwrap();
        writer.write_image_data(&[20, 40, 60]).unwrap();
    }
    bytes
}
fn jpeg_with_gps_and_orientation(value: u16) -> Vec<u8> {
    let rgb = DynamicImage::ImageRgb8(RgbImage::from_fn(7, 3, |x, y| {
        image::Rgb([(x * 30) as u8, (y * 80) as u8, 30])
    }));
    let jpeg = encode(&rgb, ImageFormat::Jpeg);
    // Real EXIF IFD0 Orientation + GPSInfo pointer; GPS IFD latitude reference
    // and three rational latitude values. Source fields are not decoder mocks.
    let mut tiff = b"II\x2a\0\x08\0\0\0".to_vec();
    tiff.extend(2u16.to_le_bytes());
    tiff.extend(0x112u16.to_le_bytes());
    tiff.extend(3u16.to_le_bytes());
    tiff.extend(1u32.to_le_bytes());
    tiff.extend(value.to_le_bytes());
    tiff.extend([0, 0]);
    tiff.extend(0x8825u16.to_le_bytes());
    tiff.extend(4u16.to_le_bytes());
    tiff.extend(1u32.to_le_bytes());
    tiff.extend(38u32.to_le_bytes());
    tiff.extend(0u32.to_le_bytes());
    tiff.extend(2u16.to_le_bytes());
    tiff.extend(1u16.to_le_bytes());
    tiff.extend(2u16.to_le_bytes());
    tiff.extend(2u32.to_le_bytes());
    tiff.extend(*b"N\0\0\0");
    tiff.extend(2u16.to_le_bytes());
    tiff.extend(5u16.to_le_bytes());
    tiff.extend(3u32.to_le_bytes());
    tiff.extend(68u32.to_le_bytes());
    tiff.extend(0u32.to_le_bytes());
    for n in [55u32, 45, 12] {
        tiff.extend(n.to_le_bytes());
        tiff.extend(1u32.to_le_bytes());
    }
    let mut exif = b"Exif\0\0".to_vec();
    exif.extend(tiff);
    let mut bytes = vec![0xff, 0xd8];
    for (marker, payload) in [
        (0xe1, exif),
        (0xfe, b"PRIVATE-CAMERA-SERIAL-TIME-GPS".to_vec()),
        (
            0xe1,
            b"http://ns.adobe.com/xap/1.0/\0<gps>PRIVATE-XMP-GPS</gps>".to_vec(),
        ),
    ] {
        bytes.extend([0xff, marker]);
        bytes.extend(u16::try_from(payload.len() + 2).unwrap().to_be_bytes());
        bytes.extend(payload);
    }
    bytes.extend(&jpeg[2..]);
    bytes.extend(b"PRIVATE-TRAILING-SOURCE-DATA");
    bytes
}
fn assert_clean_png(bytes: &[u8]) {
    let mut decoder = image::codecs::png::PngDecoder::new(Cursor::new(bytes)).unwrap();
    assert!(decoder.exif_metadata().unwrap().is_none());
    assert!(decoder.icc_profile().unwrap().is_none());
    assert!(decoder.xmp_metadata().unwrap().is_none());
    let mut offset = 8;
    while offset < bytes.len() {
        let len = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        assert!(matches!(
            &bytes[offset + 4..offset + 8],
            b"IHDR" | b"IDAT" | b"IEND"
        ));
        offset += 12 + len;
    }
    assert_eq!(offset, bytes.len());
}

#[test]
fn clean_default_removes_real_gps_exif_xmp_comments_and_trailing_bytes_after_orientation() {
    let input = jpeg_with_gps_and_orientation(6);
    assert!(
        image::codecs::jpeg::JpegDecoder::new(Cursor::new(&input))
            .unwrap()
            .exif_metadata()
            .unwrap()
            .is_some()
    );
    let result = prepare_photo(&input, PhotoChoice::default()).unwrap();
    assert!(result.metadata_stripped());
    assert_eq!(result.mime(), "image/png");
    assert_eq!(result.extension(), "png");
    assert_eq!(result.dimensions(), (3, 7));
    assert_clean_png(result.as_bytes());
    assert!(!result.as_bytes().windows(7).any(|b| b == b"PRIVATE"));
    let mut expected = image::load_from_memory(&input).unwrap();
    expected.apply_orientation(image::metadata::Orientation::Rotate90);
    assert_eq!(
        image::load_from_memory(result.as_bytes())
            .unwrap()
            .to_rgb8(),
        expected.to_rgb8()
    );
}

#[test]
fn all_eight_exif_transforms_keep_the_decoded_pixel_layout() {
    for value in 1..=8 {
        let input = jpeg_with_gps_and_orientation(value);
        let result = prepare_photo(&input, PhotoChoice::default()).unwrap();
        let mut expected = image::load_from_memory(&input).unwrap();
        expected.apply_orientation(image::metadata::Orientation::from_exif(value as u8).unwrap());
        assert_eq!(
            image::load_from_memory(result.as_bytes())
                .unwrap()
                .to_rgb8(),
            expected.to_rgb8(),
            "orientation {value}"
        );
    }
}

#[test]
fn smaller_source_never_bypasses_metadata_removal_in_clean_compression() {
    let input = tagged_png();
    let ordinary =
        codec::compress_image_with_options(&input, codec::CompressionOptions::default()).unwrap();
    assert_eq!(ordinary.reason, "original_smaller");
    assert_eq!(ordinary.data, input);
    let clean = prepare_photo(
        &input,
        PhotoChoice::Clean {
            quality: Some(85),
            max_dimension: 0,
        },
    )
    .unwrap();
    assert!(clean.as_bytes().len() > input.len());
    assert!(clean.metadata_stripped());
    assert_eq!(clean.mime(), "image/jpeg");
    let mut decoder = image::codecs::jpeg::JpegDecoder::new(Cursor::new(clean.as_bytes())).unwrap();
    assert!(decoder.exif_metadata().unwrap().is_none());
    assert!(decoder.xmp_metadata().unwrap().is_none());
    let lossless = prepare_photo(&input, PhotoChoice::default()).unwrap();
    assert_clean_png(lossless.as_bytes());
}

#[test]
fn explicit_original_requires_confirmation_and_preserves_all_bytes_and_metadata() {
    let input = jpeg_with_gps_and_orientation(6);
    assert!(matches!(
        prepare_photo(
            &input,
            PhotoChoice::Original {
                metadata_warning_acknowledged: false
            }
        ),
        Err(PhotoPreparationError::OriginalNeedsConfirmation)
    ));
    let original = prepare_photo(
        &input,
        PhotoChoice::Original {
            metadata_warning_acknowledged: true,
        },
    )
    .unwrap();
    assert_eq!(original.as_bytes(), input);
    assert!(!original.metadata_stripped());
    assert_eq!(original.mime(), "image/jpeg");
    assert_eq!(original.dimensions(), (3, 7));
}

#[test]
fn lossless_pixels_preserve_transparency_and_sixteen_bit_channels() {
    for pixels in [
        DynamicImage::ImageRgba8(RgbaImage::from_pixel(2, 3, image::Rgba([10, 20, 30, 83]))),
        DynamicImage::ImageRgb16(image::ImageBuffer::from_pixel(
            2,
            3,
            image::Rgb([513u16, 4095, 65534]),
        )),
    ] {
        let input = encode(&pixels, ImageFormat::Png);
        let result = prepare_photo(&input, PhotoChoice::default()).unwrap();
        assert_eq!(
            image::load_from_memory(result.as_bytes())
                .unwrap()
                .to_rgba16(),
            pixels.to_rgba16()
        );
        assert_clean_png(result.as_bytes());
    }
}

#[test]
fn unsupported_lossless_pixel_type_reports_encoding_failure_without_original_fallback() {
    let image = DynamicImage::ImageRgb32F(image::ImageBuffer::from_pixel(
        2,
        3,
        image::Rgb([0.2f32, 0.4, 0.6]),
    ));
    let source = encode(&image, ImageFormat::Tiff);
    assert!(matches!(
        prepare_photo(&source, PhotoChoice::default()),
        Err(PhotoPreparationError::Codec(
            CompressionError::EncodeFailed(_)
        ))
    ));
    assert_eq!(
        prepare_photo(
            &source,
            PhotoChoice::Original {
                metadata_warning_acknowledged: true
            }
        )
        .unwrap()
        .as_bytes(),
        source
    );
}

#[test]
fn static_six_format_policy_and_compressed_alpha_have_truthful_types() {
    let pixels = DynamicImage::ImageRgb8(RgbImage::from_pixel(8, 5, image::Rgb([20, 40, 60])));
    for format in [
        ImageFormat::Jpeg,
        ImageFormat::Png,
        ImageFormat::WebP,
        ImageFormat::Bmp,
        ImageFormat::Tiff,
    ] {
        let result = prepare_photo(&encode(&pixels, format), PhotoChoice::default()).unwrap();
        assert_eq!(result.mime(), "image/png");
        assert_eq!(
            image::guess_format(result.as_bytes()).unwrap(),
            ImageFormat::Png
        );
    }
    let alpha = encode(
        &DynamicImage::ImageRgba8(RgbaImage::from_pixel(8, 5, image::Rgba([20, 40, 60, 90]))),
        ImageFormat::Png,
    );
    let result = prepare_photo(
        &alpha,
        PhotoChoice::Clean {
            quality: Some(80),
            max_dimension: 0,
        },
    )
    .unwrap();
    assert_eq!(result.mime(), "image/webp");
    assert_eq!(result.extension(), "webp");
    assert_eq!(
        image::load_from_memory(result.as_bytes())
            .unwrap()
            .to_rgba8()
            .get_pixel(0, 0)[3],
        90
    );
}

#[test]
fn gif_apng_and_animated_webp_are_not_silently_flattened_or_sent_with_metadata() {
    let frame = RgbaImage::from_pixel(2, 2, image::Rgba([20, 40, 60, 90]));
    let mut gif = Vec::new();
    image::codecs::gif::GifEncoder::new(&mut gif)
        .encode_frames(vec![
            image::Frame::new(frame.clone()),
            image::Frame::new(frame),
        ])
        .unwrap();
    let mut apng = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut apng, 2, 2);
        encoder.set_color(png::ColorType::Rgba);
        encoder.set_animated(2, 0).unwrap();
        let mut writer = encoder.write_header().unwrap();
        writer
            .write_image_data(&[20, 40, 60, 90].repeat(4))
            .unwrap();
        writer
            .write_image_data(&[90, 60, 40, 20].repeat(4))
            .unwrap();
        writer.finish().unwrap();
    }
    let a = [20, 40, 60, 90].repeat(4);
    let b = [90, 60, 40, 255].repeat(4);
    let config = webp::WebPConfig::new().unwrap();
    let mut encoder = webp::AnimEncoder::new(2, 2, &config);
    encoder.add_frame(webp::AnimFrame::from_rgba(&a, 2, 2, 0));
    encoder.add_frame(webp::AnimFrame::from_rgba(&b, 2, 2, 100));
    let webp = encoder.try_encode().unwrap().to_vec();
    assert!(
        image::codecs::webp::WebPDecoder::new(Cursor::new(&webp))
            .unwrap()
            .has_animation()
    );
    for input in [gif, apng, webp] {
        assert!(matches!(
            prepare_photo(&input, PhotoChoice::default()),
            Err(PhotoPreparationError::AnimationNeedsOriginal)
        ));
        let original = prepare_photo(
            &input,
            PhotoChoice::Original {
                metadata_warning_acknowledged: true,
            },
        )
        .unwrap();
        assert_eq!(original.as_bytes(), input);
        assert!(!original.metadata_stripped());
    }
}

#[test]
fn multipage_tiff_requires_an_explicit_original_without_losing_pages() {
    let mut bytes = Cursor::new(Vec::new());
    {
        let mut tiff = tiff::encoder::TiffEncoder::new(&mut bytes).unwrap();
        for color in [[20u8, 40, 60], [60, 40, 20]] {
            tiff.write_image::<tiff::encoder::colortype::RGB8>(2, 2, &color.repeat(4))
                .unwrap();
        }
    }
    assert!(matches!(
        prepare_photo(bytes.get_ref(), PhotoChoice::default()),
        Err(PhotoPreparationError::MultipageNeedsOriginal)
    ));
    assert_eq!(
        prepare_photo(
            bytes.get_ref(),
            PhotoChoice::Original {
                metadata_warning_acknowledged: true
            }
        )
        .unwrap()
        .as_bytes(),
        bytes.get_ref()
    );
}

#[test]
fn resize_never_upscales_and_quality_changes_output() {
    let pixels = DynamicImage::ImageRgb8(RgbImage::from_fn(256, 128, |x, y| {
        image::Rgb([x as u8, y as u8, (x ^ y) as u8])
    }));
    let input = encode(&pixels, ImageFormat::Png);
    let low = prepare_photo(
        &input,
        PhotoChoice::Clean {
            quality: Some(40),
            max_dimension: 0,
        },
    )
    .unwrap();
    let high = prepare_photo(
        &input,
        PhotoChoice::Clean {
            quality: Some(95),
            max_dimension: 0,
        },
    )
    .unwrap();
    assert!(low.as_bytes().len() < high.as_bytes().len());
    let small = prepare_photo(
        &input,
        PhotoChoice::Clean {
            quality: None,
            max_dimension: 64,
        },
    )
    .unwrap();
    assert_eq!(small.dimensions(), (64, 32));
    assert_eq!(
        prepare_photo(
            &input,
            PhotoChoice::Clean {
                quality: None,
                max_dimension: 512
            }
        )
        .unwrap()
        .dimensions(),
        (256, 128)
    );
}

#[test]
fn limits_corruption_and_encoder_failure_never_fall_back_to_source() {
    assert!(prepare_photo(b"not an image", PhotoChoice::default()).is_err());
    assert!(matches!(
        prepare_photo(&vec![0; MAX_INPUT_BYTES + 1], PhotoChoice::default()),
        Err(PhotoPreparationError::Codec(
            CompressionError::InputTooLarge
        ))
    ));
    let bytes = tagged_png();
    for quality in [0, 101] {
        assert!(matches!(
            prepare_photo(
                &bytes,
                PhotoChoice::Clean {
                    quality: Some(quality),
                    max_dimension: 0
                }
            ),
            Err(PhotoPreparationError::Codec(
                CompressionError::InvalidQuality(_)
            ))
        ));
    }
    assert!(matches!(
        prepare_photo(
            &bytes,
            PhotoChoice::Clean {
                quality: None,
                max_dimension: 63
            }
        ),
        Err(PhotoPreparationError::Codec(
            CompressionError::InvalidMaxDimension(_)
        ))
    ));
    for quality in [None, Some(80)] {
        assert!(matches!(
            prepare_with_limit(
                &bytes,
                PhotoChoice::Clean {
                    quality,
                    max_dimension: 0
                },
                1
            ),
            Err(PhotoPreparationError::OutputTooLarge)
        ));
    }
    let mut oversized = bytes;
    oversized[16..20].copy_from_slice(&20_000u32.to_be_bytes());
    let mut crc = !0u32;
    for byte in &oversized[12..29] {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb88320u32 & 0u32.wrapping_sub(crc & 1));
        }
    }
    oversized[29..33].copy_from_slice(&(!crc).to_be_bytes());
    assert!(matches!(
        prepare_photo(&oversized, PhotoChoice::default()),
        Err(PhotoPreparationError::Codec(
            CompressionError::ImageTooLarge
        ))
    ));
}
