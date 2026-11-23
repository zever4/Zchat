fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "e2ee")]
    {
        // Go and Rust must generate their bindings from the same wire contract.
        // Native protocol generators need an unambiguous sibling include path.
        // Windows builds from a WSL share also need a mapped working drive.
        let manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
        let proto_dir = manifest_dir
            .parent()
            .ok_or("missing workspace directory")?
            .join("proto");
        let protocols = [
            proto_dir.join("messenger.proto"),
            proto_dir.join("e2ee.proto"),
            proto_dir.join("e2ee_epoch.proto"),
        ];
        for path in &protocols {
            println!("cargo:rerun-if-changed={}", path.display());
        }
        tonic_prost_build::configure()
            .build_server(false)
            .skip_debug([
                ".zchat.e2ee.v1.ReplaceOwnDeviceRequest",
                ".zchat.e2ee.v2.AttachmentMessage",
            ])
            .compile_protos(&protocols, &[proto_dir])?;
    }
    Ok(())
}
