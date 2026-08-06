use std::path::Path;

#[test]
fn runtime_consumes_published_ethos_interfaces_without_emitter_ownership() {
    let manifest = include_str!("../Cargo.toml");
    for required in [
        "d57830076840f0e1ef89352823d7b0356a7c96df",
        "c14fc141ea46652eb1bfbcd6138d08578864e6c5",
        "d5a4a545e61dafec30667f2af38ca503ab6d6d3f",
        "80c7b17f7ad3cf547d2624c6a243e5de5f85c9f3",
    ] {
        assert!(
            manifest.contains(required),
            "missing exact publisher {required}"
        );
    }
    for forbidden in ["branch =", "schema-rust", "build-dependencies", "nota-text"] {
        assert!(
            !manifest.contains(forbidden),
            "retired manifest ownership survived: {forbidden}"
        );
    }
    for retired in ["build.rs", "schema", "src/schema", "src/schema_daemon.rs"] {
        assert!(
            !Path::new(retired).exists(),
            "retired path survived: {retired}"
        );
    }
}

#[test]
fn crate_root_publishes_no_readable_wire_shadow() {
    let crate_root = include_str!("../src/lib.rs");
    assert!(!crate_root.contains("pub mod schema"));
    assert!(!crate_root.contains("pub use signal_mirror"));
    assert!(!crate_root.contains("pub use meta_signal_mirror"));
}
