//! Native photo preprocessing -> validated private attachment input. No account,
//! transport/Signal keys or renderer-facing arbitrary metadata flag.
use super::attachment_payload::{AttachmentDraft, MAX_MEDIA_BYTES};
use crate::{
    compression::{PhotoChoice, PhotoPreparationError, prepare_photo},
    proto::e2ee_v2::AttachmentKind,
};
use zeroize::Zeroizing;

#[derive(Debug, thiserror::Error)]
pub enum PhotoAttachmentError {
    #[error(transparent)]
    Preparation(#[from] PhotoPreparationError),
    #[error("invalid private photo metadata")]
    Metadata,
    #[error("clean photo exceeds inline media limits; choose compression or smaller dimensions")]
    MediaTooLarge,
}

/// Native-only owned handoff. Consume once into the existing immutable
/// preparation path; network retries must use its retained UUID/ciphertext.
pub struct PreparedPhotoAttachment {
    draft: AttachmentDraft,
    plaintext: Zeroizing<Vec<u8>>,
}
impl PreparedPhotoAttachment {
    pub fn into_input(self) -> (AttachmentDraft, Zeroizing<Vec<u8>>) {
        (self.draft, self.plaintext)
    }
}

pub fn prepare_photo_attachment(
    source: &[u8],
    filename: Zeroizing<String>,
    caption: Zeroizing<String>,
    choice: PhotoChoice,
) -> Result<PreparedPhotoAttachment, PhotoAttachmentError> {
    // Reject paths/control chars/invalid caption before opening a codec. Using
    // the ordinary file draft here validates only the source metadata/size.
    let mut draft = AttachmentDraft::file(filename, caption);
    draft
        .validate(source.len())
        .map_err(|_| PhotoAttachmentError::Metadata)?;
    let prepared = prepare_photo(source, choice)?;
    let clean = prepared.metadata_stripped();
    if clean && prepared.as_bytes().len() > MAX_MEDIA_BYTES {
        return Err(PhotoAttachmentError::MediaTooLarge);
    }
    let inline = prepared.as_bytes().len() <= MAX_MEDIA_BYTES
        && matches!(
            prepared.mime(),
            "image/jpeg" | "image/png" | "image/webp" | "image/gif"
        );
    let stem = draft
        .filename
        .rsplit_once('.')
        .filter(|(stem, _)| !stem.is_empty())
        .map_or(draft.filename.as_str(), |(stem, _)| stem);
    draft.filename = Zeroizing::new(format!("{stem}.{}", prepared.extension()));
    if inline {
        draft.kind = AttachmentKind::Photo;
        draft.mime_type = Zeroizing::new(prepared.mime().into());
        draft.photo_metadata_stripped = Some(clean);
    }
    let plaintext = Zeroizing::new(prepared.into_bytes());
    draft
        .validate(plaintext.len())
        .map_err(|_| PhotoAttachmentError::Metadata)?;
    Ok(PreparedPhotoAttachment { draft, plaintext })
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{DynamicImage, ImageFormat, RgbImage};
    use std::io::Cursor;
    fn source(format: ImageFormat) -> Vec<u8> {
        let mut out = Cursor::new(Vec::new());
        DynamicImage::ImageRgb8(RgbImage::from_pixel(2, 3, image::Rgb([20, 40, 60])))
            .write_to(&mut out, format)
            .unwrap();
        out.into_inner()
    }
    #[test]
    fn native_handoff_binds_actual_type_name_and_sanitizer_result() {
        let src = source(ImageFormat::Jpeg);
        let (draft, bytes) = prepare_photo_attachment(
            &src,
            Zeroizing::new("capture.not-jpeg".into()),
            Zeroizing::new("caption".into()),
            PhotoChoice::default(),
        )
        .unwrap()
        .into_input();
        assert_eq!(draft.kind, AttachmentKind::Photo);
        assert_eq!(draft.filename.as_str(), "capture.png");
        assert_eq!(draft.mime_type.as_str(), "image/png");
        assert_eq!(draft.photo_metadata_stripped, Some(true));
        assert_eq!(image::guess_format(&bytes).unwrap(), ImageFormat::Png);
        let (draft, bytes) = prepare_photo_attachment(
            &src,
            Zeroizing::new("capture.bad".into()),
            Zeroizing::new(String::new()),
            PhotoChoice::Original {
                metadata_warning_acknowledged: true,
            },
        )
        .unwrap()
        .into_input();
        assert_eq!(draft.filename.as_str(), "capture.jpg");
        assert_eq!(draft.photo_metadata_stripped, Some(false));
        assert_eq!(bytes.as_slice(), src);
    }
    #[test]
    fn original_noninline_formats_and_large_photos_become_files_without_false_clean_flag() {
        for format in [ImageFormat::Bmp, ImageFormat::Tiff] {
            let src = source(format);
            let (draft, bytes) = prepare_photo_attachment(
                &src,
                Zeroizing::new("original.bin".into()),
                Zeroizing::new(String::new()),
                PhotoChoice::Original {
                    metadata_warning_acknowledged: true,
                },
            )
            .unwrap()
            .into_input();
            assert_eq!(draft.kind, AttachmentKind::File);
            assert_eq!(draft.mime_type.as_str(), "application/octet-stream");
            assert_eq!(draft.photo_metadata_stripped, None);
            assert_eq!(bytes.as_slice(), src);
        }
        let mut src = source(ImageFormat::Png);
        src.resize(MAX_MEDIA_BYTES + 1, 0);
        let (draft, bytes) = prepare_photo_attachment(
            &src,
            Zeroizing::new("large.png".into()),
            Zeroizing::new(String::new()),
            PhotoChoice::Original {
                metadata_warning_acknowledged: true,
            },
        )
        .unwrap()
        .into_input();
        assert_eq!(draft.kind, AttachmentKind::File);
        assert_eq!(draft.photo_metadata_stripped, None);
        assert_eq!(bytes.as_slice(), src);
    }
    #[test]
    fn clean_oversized_png_requires_smaller_output_instead_of_original_or_file_fallback() {
        let pixels = DynamicImage::ImageRgb8(RgbImage::from_fn(2048, 2048, |x, y| {
            // Deterministic high-entropy pixels keep both input/output above the
            // inline cap without an excessive-dimensions or oversized-input error.
            let mut n = (u64::from(x) << 32) | u64::from(y);
            n = n.wrapping_add(0x9e3779b97f4a7c15);
            n = (n ^ (n >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            n = (n ^ (n >> 27)).wrapping_mul(0x94d049bb133111eb);
            n ^= n >> 31;
            image::Rgb([n as u8, (n >> 8) as u8, (n >> 16) as u8])
        }));
        let mut src = Cursor::new(Vec::new());
        pixels.write_to(&mut src, ImageFormat::Png).unwrap();
        assert!(
            src.get_ref().len() > MAX_MEDIA_BYTES
                && src.get_ref().len() < crate::compression::MAX_INPUT_BYTES
        );
        assert!(matches!(
            prepare_photo_attachment(
                src.get_ref(),
                Zeroizing::new("large.png".into()),
                Zeroizing::new(String::new()),
                PhotoChoice::default()
            ),
            Err(PhotoAttachmentError::MediaTooLarge)
        ));
    }
    #[test]
    fn paths_bad_names_and_missing_original_consent_are_rejected() {
        let src = source(ImageFormat::Png);
        for name in [
            "../photo.png",
            "C:\\secret.png",
            "bad\0name.png",
            "photo.png.",
        ] {
            assert!(matches!(
                prepare_photo_attachment(
                    &src,
                    Zeroizing::new(name.into()),
                    Zeroizing::new(String::new()),
                    PhotoChoice::default()
                ),
                Err(PhotoAttachmentError::Metadata)
            ));
        }
        assert!(matches!(
            prepare_photo_attachment(
                &src,
                Zeroizing::new("original.png".into()),
                Zeroizing::new(String::new()),
                PhotoChoice::Original {
                    metadata_warning_acknowledged: false
                }
            ),
            Err(PhotoAttachmentError::Preparation(
                PhotoPreparationError::OriginalNeedsConfirmation
            ))
        ));
    }
}
