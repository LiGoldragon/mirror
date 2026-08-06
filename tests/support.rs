#![allow(dead_code)]

use mirror::Engine;
use signal_mirror::{
    z2VPuU, z2VSAK, z2VTXE, z2VTq5, z2VTqL, z2VUKn, z2VUwg, z2VUxk, z2VVny, z2VcqM, z2Ve8p,
};
use signal_standard::z2VSyM;

pub fn store(name: &str) -> z2Ve8p {
    z2Ve8p::new(name.to_owned())
}

pub fn digest(seed: u8) -> z2VSyM {
    z2VSyM::new(format!("{seed:02x}").repeat(32))
}

pub fn head(sequence: u64, seed: u8) -> z2VcqM {
    z2VcqM {
        field_0: z2VSAK::new(sequence),
        field_1: digest(seed),
    }
}

pub fn envelope(sequence: u64, previous: Option<u8>, seed: u8) -> z2VPuU {
    z2VPuU {
        field_0: z2VSAK::new(sequence),
        field_1: previous.map(digest),
        field_2: digest(seed),
        field_3: z2VUwg::from_octets(&[seed; 4]),
    }
}

pub fn append(name: &str, expected: Option<z2VcqM>, entries: Vec<z2VPuU>) -> z2VVny {
    z2VVny::z2VVjQ(z2VTq5 {
        field_0: store(name),
        field_1: expected,
        field_2: entries,
    })
}

pub fn artifact(name: &str, checkpoint: u64, covered: u64) -> z2VTXE {
    z2VTXE {
        field_0: store(name),
        field_1: z2VUKn::new(checkpoint),
        field_2: z2VSAK::new(covered),
        field_3: digest(checkpoint as u8),
        field_4: z2VUxk::from_octets(&[checkpoint as u8; 8]),
    }
}

pub fn register(engine: &mut Engine, name: &str, addressing: meta_signal_mirror::z2VMYP) {
    let output = engine.handle_meta(meta_signal_mirror::z2VWt5::z2VWC2(
        meta_signal_mirror::z2VWBn {
            field_0: store(name),
            field_1: addressing,
        },
    ));
    assert!(matches!(output, meta_signal_mirror::z2VUH6::z2VSig(_)));
}

pub async fn observed_head(engine: &mut Engine, name: &str) -> Option<z2VcqM> {
    match engine
        .handle(z2VVny::z2VZ8E(signal_mirror::z2Vdqa::new(Some(store(
            name,
        )))))
        .await
    {
        z2VTqL::z2VMR1(listing) => listing
            .field_0
            .into_iter()
            .find(|entry| entry.field_0 == store(name))
            .and_then(|entry| entry.field_1),
        other => panic!("unexpected head output: {other:?}"),
    }
}
