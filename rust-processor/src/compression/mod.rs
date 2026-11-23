#[path = "compression.rs"]
mod codec;
mod photo;

pub use photo::{PhotoChoice, PhotoPreparationError, PreparedPhoto, prepare_photo};

pub use codec::{
    CompressedImage, CompressionOptions, MAX_INPUT_BYTES, MAX_PIXELS, compress_image,
    compress_image_with_options,
};
