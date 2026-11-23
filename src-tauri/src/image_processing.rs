use rust_processor::compression::{
    CompressionOptions, MAX_INPUT_BYTES, compress_image_with_options,
};
use rust_processor::errors::CompressionError;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub struct ImageProcessor {
    gate: Arc<Semaphore>,
}

impl Default for ImageProcessor {
    fn default() -> Self {
        Self {
            gate: Arc::new(Semaphore::new(1)),
        }
    }
}

impl ImageProcessor {
    pub async fn prepare(
        &self,
        data: &[u8],
        options: CompressionOptions,
    ) -> Result<Vec<u8>, String> {
        if data.len() > MAX_INPUT_BYTES {
            return Err("Для сжатия выберите изображение до 20 МБ".into());
        }
        let permit = self.gate.clone().try_acquire_owned().map_err(|_| {
            "Другое изображение ещё обрабатывается. Повторите через несколько секунд".to_string()
        })?;
        let data = data.to_vec();
        // The permit belongs to the worker: closing a page cannot admit another
        // encoder while a non-interruptible native codec is still finishing.
        tauri::async_runtime::spawn_blocking(move || {
            let _permit = permit;
            let image = compress_image_with_options(&data, options).map_err(|error| match error {
                CompressionError::InputTooLarge => "Для сжатия выберите изображение до 20 МБ",
                CompressionError::ImageTooLarge => "Изображение слишком большое для обработки: до 24 Мп, сторона до 12000 пикселей",
                CompressionError::UnsupportedFormat => "Формат не поддерживается для сжатия. Можно отправить оригинал файлом",
                CompressionError::InvalidQuality(_) | CompressionError::InvalidMaxDimension(_) => "Недопустимые параметры сжатия",
                _ => "Не удалось обработать изображение. Можно отправить оригинал файлом",
            }.to_string())?;
            if image.data.len() > MAX_INPUT_BYTES {
                return Err("Результат слишком большой. Уменьшите качество или размеры либо отправьте оригинал файлом".into());
            }
            #[derive(Serialize)]
            struct Metadata<'a> {
                version: u8, mime: &'a str, extension: &'a str, width: u32, height: u32,
                changed: bool, reason: &'a str,
            }
            let metadata = serde_json::to_vec(&Metadata { version: 1, mime: image.mime(), extension: image.extension(),
                width: image.width, height: image.height, changed: image.changed, reason: image.reason })
                .map_err(|_| "Не удалось подготовить изображение".to_string())?;
            // Binary IPC in both directions avoids expanding image bytes into
            // millions of JSON numbers. A small versioned header describes MIME.
            let mut result = Vec::with_capacity(4 + metadata.len() + image.data.len());
            result.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
            result.extend_from_slice(&metadata);
            result.extend_from_slice(&image.data);
            Ok(result)
        }).await.map_err(|_| "Обработка изображения завершилась с ошибкой".to_string())?
    }
}

pub fn options(headers: &tauri::http::HeaderMap) -> Result<CompressionOptions, String> {
    let parse = |name: &str, default: u32| -> Result<u32, String> {
        headers
            .get(name)
            .map(|value| {
                value
                    .to_str()
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| "Недопустимые параметры сжатия".to_string())
            })
            .unwrap_or(Ok(default))
    };
    let quality = u8::try_from(parse("x-zchat-quality", 85)?)
        .map_err(|_| "Недопустимое качество".to_string())?;
    let max_dimension = parse("x-zchat-max-dimension", 0)?;
    if !(1..=100).contains(&quality)
        || (max_dimension != 0 && !(64..=12000).contains(&max_dimension))
    {
        return Err("Недопустимые параметры сжатия".into());
    }
    Ok(CompressionOptions {
        quality,
        max_dimension,
        keep_smaller: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_processing_headers_are_rejected() {
        for (name, value) in [
            ("x-zchat-quality", "0"),
            ("x-zchat-quality", "999"),
            ("x-zchat-quality", "NaN"),
            ("x-zchat-max-dimension", "-1"),
            ("x-zchat-max-dimension", "12001"),
        ] {
            let mut headers = tauri::http::HeaderMap::new();
            headers.insert(name, value.parse().unwrap());
            assert!(options(&headers).is_err());
        }
    }

    #[tokio::test]
    async fn occupied_worker_rejects_before_decoding_and_releases_for_retry() {
        let processor = ImageProcessor::default();
        let permit = processor.gate.clone().acquire_owned().await.unwrap();
        assert!(
            processor
                .prepare(b"invalid", CompressionOptions::default())
                .await
                .unwrap_err()
                .contains("Другое изображение")
        );
        drop(permit);
        assert!(
            processor
                .prepare(b"invalid", CompressionOptions::default())
                .await
                .unwrap_err()
                .contains("Не удалось обработать")
        );
    }
}
