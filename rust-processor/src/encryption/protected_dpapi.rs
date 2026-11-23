use super::{KeyProtector, StoreError, StoreResult};
use zeroize::Zeroizing;

pub(super) struct WindowsDpapi;

pub(super) fn require_platform() -> StoreResult<()> {
    if cfg!(windows) {
        Ok(())
    } else {
        Err(StoreError::UnsupportedPlatform)
    }
}

#[cfg(not(windows))]
impl KeyProtector for WindowsDpapi {
    fn wrap(&self, _: &[u8], _: &[u8; 32]) -> StoreResult<Vec<u8>> {
        Err(StoreError::UnsupportedPlatform)
    }
    fn unwrap(&self, _: &[u8], _: &[u8; 32]) -> StoreResult<Zeroizing<Vec<u8>>> {
        Err(StoreError::UnsupportedPlatform)
    }
}

#[cfg(windows)]
mod native {
    use super::*;
    use windows_sys::Win32::{
        Foundation::LocalFree,
        Security::Cryptography::{
            CRYPT_INTEGER_BLOB, CRYPTPROTECT_UI_FORBIDDEN, CryptProtectData, CryptUnprotectData,
        },
    };
    use zeroize::Zeroize;

    impl KeyProtector for WindowsDpapi {
        fn wrap(&self, key: &[u8], namespace: &[u8; 32]) -> StoreResult<Vec<u8>> {
            if key.len() != 32 {
                return Err(StoreError::Protection);
            }
            Ok(transform(key, namespace, true)?.to_vec())
        }

        fn unwrap(&self, wrapped: &[u8], namespace: &[u8; 32]) -> StoreResult<Zeroizing<Vec<u8>>> {
            let key = transform(wrapped, namespace, false)?;
            if key.len() != 32 {
                return Err(StoreError::Protection);
            }
            Ok(key)
        }
    }

    // Keep the allocation alive until copied, wipe before LocalFree on every path.
    struct Output(CRYPT_INTEGER_BLOB);
    impl Drop for Output {
        fn drop(&mut self) {
            if !self.0.pbData.is_null() {
                // SAFETY: successful DPAPI calls allocate cbData bytes via LocalAlloc.
                unsafe {
                    std::slice::from_raw_parts_mut(self.0.pbData, self.0.cbData as usize).zeroize();
                    LocalFree(self.0.pbData.cast());
                }
            }
        }
    }

    fn transform(
        input: &[u8],
        namespace: &[u8; 32],
        protect: bool,
    ) -> StoreResult<Zeroizing<Vec<u8>>> {
        if input.is_empty() || input.len() > 16384 {
            return Err(StoreError::Protection);
        }
        let source = CRYPT_INTEGER_BLOB {
            cbData: input.len() as u32,
            pbData: input.as_ptr().cast_mut(),
        };
        let entropy = CRYPT_INTEGER_BLOB {
            cbData: 32,
            pbData: namespace.as_ptr().cast_mut(),
        };
        let mut output = Output(CRYPT_INTEGER_BLOB::default());
        // SAFETY: input buffers remain live and DPAPI treats them as input only.
        // No LOCAL_MACHINE flag, prompts, descriptions, passwords or JWTs are used.
        let ok = unsafe {
            if protect {
                CryptProtectData(
                    &source,
                    std::ptr::null(),
                    &entropy,
                    std::ptr::null(),
                    std::ptr::null(),
                    CRYPTPROTECT_UI_FORBIDDEN,
                    &mut output.0,
                )
            } else {
                CryptUnprotectData(
                    &source,
                    std::ptr::null_mut(),
                    &entropy,
                    std::ptr::null(),
                    std::ptr::null(),
                    CRYPTPROTECT_UI_FORBIDDEN,
                    &mut output.0,
                )
            }
        };
        if ok == 0 || output.0.pbData.is_null() || output.0.cbData == 0 || output.0.cbData > 16384 {
            return Err(StoreError::Protection);
        }
        // SAFETY: output allocation has been checked and is owned until Output::drop.
        Ok(Zeroizing::new(unsafe {
            std::slice::from_raw_parts(output.0.pbData, output.0.cbData as usize).to_vec()
        }))
    }
}
