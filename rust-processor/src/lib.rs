#[cfg(feature = "compression")]
pub mod compression;
#[cfg(feature = "e2ee")]
pub mod encryption;
pub mod errors;

#[cfg(feature = "e2ee")]
pub mod proto {
    pub mod encryption {
        tonic::include_proto!("messenger");
    }

    // Keep protobuf sibling namespaces aligned for generated cross-package refs.
    // Existing registry consumers retain their Rust import path through aliases.
    mod e2ee_wire {
        pub mod v1 {
            tonic::include_proto!("zchat.e2ee.v1");
        }
        pub mod v2 {
            tonic::include_proto!("zchat.e2ee.v2");
        }
    }
    pub use e2ee_wire::{v1 as e2ee, v2 as e2ee_v2};
}
