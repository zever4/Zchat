use super::Reason;
use std::{
    io::{self, Write},
    path::Path,
};

fn safe_basename(name: &str) -> bool {
    let stem = name
        .split('.')
        .next()
        .unwrap_or_default()
        .trim_end()
        .to_ascii_uppercase();
    !name.is_empty()
        && name.len() <= 255
        && name.trim() == name
        && !name.ends_with('.')
        && !name.chars().any(|c| {
            c.is_control()
                || "<>:\"/\\|?*".contains(c)
                || matches!(c, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        })
        && !matches!(
            stem.as_str(),
            "CON" | "NUL" | "AUX" | "PRN" | "CONIN$" | "CONOUT$"
        )
        && !["COM", "LPT"].iter().any(|prefix| {
            stem.strip_prefix(prefix).is_some_and(|tail| {
                matches!(
                    tail,
                    "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
                )
            })
        })
}

pub(super) fn write_copy(
    path: &Path,
    bytes: &[u8],
    check: impl Fn() -> Result<(), Reason>,
) -> Result<(), Reason> {
    check()?;
    if !path.is_absolute()
        || !path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(safe_basename)
    {
        return Err(Reason::AttachmentDestination);
    }
    let parent = path.parent().ok_or(Reason::AttachmentDestination)?;
    let mut pending = tempfile::Builder::new()
        .prefix(".zchat-export-")
        .tempfile_in(parent)
        .map_err(save_error)?;
    for part in bytes.chunks(64 * 1024) {
        check()?;
        pending.write_all(part).map_err(save_error)?;
    }
    pending.as_file().sync_all().map_err(save_error)?;
    check()?;
    // Never replace an existing file, including one created while the copy was written.
    pending
        .persist_noclobber(path)
        .map_err(|e| save_error(e.error))?;
    Ok(())
}

fn save_error(error: io::Error) -> Reason {
    if error.kind() == io::ErrorKind::AlreadyExists {
        Reason::AttachmentExists
    } else {
        Reason::AttachmentDestination
    }
}

#[cfg(windows)]
pub(super) async fn choose(
    window: &tauri::WebviewWindow,
    suggested_name: &str,
    cancel: &tokio_util::sync::CancellationToken,
    session: &tokio_util::sync::CancellationToken,
) -> Result<Option<std::path::PathBuf>, Reason> {
    let (sender, result) = tokio::sync::oneshot::channel();
    let owner = window.clone();
    let name = if safe_basename(suggested_name) {
        suggested_name
    } else {
        "Вложение.bin"
    };
    let name: Vec<u16> = name.encode_utf16().chain([0]).collect();
    let cancel = cancel.clone();
    let session = session.clone();
    window
        .run_on_main_thread(move || {
            let selected = if cancel.is_cancelled() || session.is_cancelled() {
                Err(Reason::Stale)
            } else {
                owner
                    .hwnd()
                    .map_err(|_| Reason::AttachmentDestination)
                    .and_then(|owner| picker(owner, &name))
            };
            let _ = sender.send(selected);
        })
        .map_err(|_| Reason::AttachmentDestination)?;
    result.await.map_err(|_| Reason::Runtime)?
}

#[cfg(windows)]
fn picker(
    hwnd: windows::Win32::Foundation::HWND,
    name: &[u16],
) -> Result<Option<std::path::PathBuf>, Reason> {
    use std::{ffi::OsString, os::windows::ffi::OsStringExt};
    use windows::{
        Win32::{
            Foundation::ERROR_CANCELLED,
            System::Com::{
                CLSCTX_INPROC_SERVER, COINIT_APARTMENTTHREADED, CoCreateInstance, CoInitializeEx,
                CoTaskMemFree, CoUninitialize,
            },
            UI::Shell::{
                FOS_DONTADDTORECENT, FOS_FORCEFILESYSTEM, FOS_NOCHANGEDIR, FOS_NODEREFERENCELINKS,
                FOS_NOTESTFILECREATE, FOS_OVERWRITEPROMPT, FileSaveDialog, IFileSaveDialog,
                SIGDN_FILESYSPATH,
            },
        },
        core::{PCWSTR, w},
    };
    struct Com;
    impl Drop for Com {
        fn drop(&mut self) {
            unsafe { CoUninitialize() };
        }
    }
    let error = |_| Reason::AttachmentDestination;
    // The COM objects and pointers live on the window's STA thread and all
    // strings passed to the Shell are NUL-terminated for the call duration.
    unsafe {
        CoInitializeEx(None, COINIT_APARTMENTTHREADED)
            .ok()
            .map_err(error)?;
        let _com = Com;
        let dialog: IFileSaveDialog =
            CoCreateInstance(&FileSaveDialog, None, CLSCTX_INPROC_SERVER).map_err(error)?;
        dialog
            .SetOptions(
                (dialog.GetOptions().map_err(error)? & !FOS_OVERWRITEPROMPT)
                    | FOS_FORCEFILESYSTEM
                    | FOS_NOCHANGEDIR
                    | FOS_DONTADDTORECENT
                    | FOS_NODEREFERENCELINKS
                    | FOS_NOTESTFILECREATE,
            )
            .map_err(error)?;
        dialog
            .SetTitle(w!("Сохранить незашифрованную копию"))
            .map_err(error)?;
        dialog.SetFileName(PCWSTR(name.as_ptr())).map_err(error)?;
        match dialog.Show(Some(hwnd)) {
            Ok(()) => {}
            Err(e) if e.code() == ERROR_CANCELLED.to_hresult() => return Ok(None),
            Err(_) => return Err(Reason::AttachmentDestination),
        }
        let value = dialog
            .GetResult()
            .and_then(|item| item.GetDisplayName(SIGDN_FILESYSPATH))
            .map_err(error)?;
        let path = OsString::from_wide(value.as_wide());
        CoTaskMemFree(Some(value.0.cast()));
        Ok(Some(path.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exports_exact_bytes_without_replacing_existing_files() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("фото.png");
        assert!(write_copy(&path, b"exact original", || Ok(())).is_ok());
        assert_eq!(
            write_copy(&path, b"replacement", || Ok(())),
            Err(Reason::AttachmentExists)
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"exact original");
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
        for name in [
            "../a",
            "C:stream",
            "CON.txt",
            "CoM¹.log",
            "name. ",
            "false\u{202e}gnp.exe",
        ] {
            assert!(!safe_basename(name), "{name}");
        }
    }

    #[test]
    fn cancelled_staged_export_has_no_published_or_temporary_file() {
        use std::cell::Cell;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("out.png");
        let checks = Cell::new(0);
        let result = write_copy(&path, b"already staged", || {
            checks.set(checks.get() + 1);
            if checks.get() == 3 {
                Err(Reason::Stale)
            } else {
                Ok(())
            }
        });
        assert_eq!(result, Err(Reason::Stale));
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }
}
