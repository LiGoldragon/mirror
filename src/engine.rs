//! Mirror's single-writer runtime engine.
//!
//! Every state-bearing request follows the same visible law: load durable
//! ledger state, decide purely, commit, then acknowledge. The Interface meets
//! that durable boundary directly.

use meta_signal_mirror::{z2VUH6, z2VWt5, z2VYSE, z2VbDA, z2Vbcq};
use signal_mirror::{z2VMCw, z2VTqL, z2VVny, z2Vc3D};

use crate::config::Configuration;
use crate::decision::{AppendDecision, CheckpointDecision, ObjectNoticeDecision};
use crate::error::Result;
use crate::ledger::ContentAddressing;
use crate::store::Store;

pub struct Engine {
    store: Store,
}

impl Engine {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    pub fn open(configuration: &Configuration) -> Result<Self> {
        Ok(Self::new(Store::open(configuration.storage_path())?))
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub async fn handle(&mut self, input: z2VVny) -> z2VTqL {
        match input {
            z2VVny::z2VVjQ(request) => match self.store.check_append(request) {
                Ok(checked) => match checked.into_decision() {
                    AppendDecision::Accept(suffix) => self
                        .store
                        .persist_suffix(&suffix)
                        .map(z2VTqL::z2VXSq)
                        .unwrap_or_else(Self::fault),
                    AppendDecision::Duplicate(receipt) => z2VTqL::z2VXSq(receipt),
                    AppendDecision::Refuse(rejection) => z2VTqL::z2VX2Y(rejection),
                },
                Err(error) => Self::fault(error),
            },
            z2VVny::z2VNu6(artifact) => match self.store.check_checkpoint(artifact) {
                Ok(checked) => match checked.into_decision() {
                    CheckpointDecision::Accept(artifact) => self
                        .store
                        .persist_checkpoint(&artifact)
                        .map(z2VTqL::z2VaSa)
                        .unwrap_or_else(Self::fault),
                    CheckpointDecision::Duplicate(receipt) => z2VTqL::z2VaSa(receipt),
                    CheckpointDecision::Refuse(rejection) => z2VTqL::z2VWHb(rejection),
                },
                Err(error) => Self::fault(error),
            },
            z2VVny::z2VaYk(notice) => match self.store.check_object_notice(notice) {
                Ok(checked) => match checked.into_decision() {
                    ObjectNoticeDecision::Accept(receipt) => z2VTqL::z2VR8x(receipt),
                    ObjectNoticeDecision::Refuse(rejection) => z2VTqL::z2VSxB(rejection),
                },
                Err(error) => Self::fault(error),
            },
            z2VVny::z2VdHF(query) => match self.store.load_restore(&query) {
                Ok(Ok(bundle)) => z2VTqL::z2VVve(bundle),
                Ok(Err(rejection)) => z2VTqL::z2VLCz(rejection),
                Err(error) => Self::fault(error),
            },
            z2VVny::z2VZ8E(query) => self
                .store
                .load_heads(&query)
                .map(z2VTqL::z2VMR1)
                .unwrap_or_else(Self::fault),
        }
    }

    pub fn handle_meta(&mut self, input: z2VWt5) -> z2VUH6 {
        match input {
            z2VWt5::z2VNSh(configuration) => z2VUH6::z2VQdi(meta_signal_mirror::z2VNz1::new(
                configuration.into_payload(),
            )),
            z2VWt5::z2VWC2(registration) => self.register_store(registration),
            z2VWt5::z2VXXR(retirement) => self.retire_store(retirement.into_payload()),
            z2VWt5::z2Vctc(order) => self.set_retention(order),
            z2VWt5::z2VMxr(_) => self.observe_registry(),
        }
    }

    fn register_store(&mut self, registration: meta_signal_mirror::z2VWBn) -> z2VUH6 {
        if !Store::name_is_keyable(&registration.field_0) {
            return Self::meta_rejection(
                z2VYSE::z2VPC3,
                "store name carries the key separator '/'",
            );
        }
        let listing = match self.store.load_registry() {
            Ok(listing) => listing,
            Err(error) => return Self::meta_rejection(z2VYSE::z2VMiY, &error.to_string()),
        };
        if listing
            .payload()
            .iter()
            .any(|registered| registered.payload() == &registration.field_0)
        {
            return Self::meta_rejection(z2VYSE::z2VY7i, "store is already registered");
        }
        let store = registration.field_0;
        let addressing = ContentAddressing::from_meta(&registration.field_1);
        match self.store.register_store(&store, addressing) {
            Ok(()) => z2VUH6::z2VSig(meta_signal_mirror::z2VbpU::new(store)),
            Err(error) => Self::meta_rejection(z2VYSE::z2VMiY, &error.to_string()),
        }
    }

    fn retire_store(&mut self, store: signal_mirror::z2Ve8p) -> z2VUH6 {
        let listing = match self.store.load_registry() {
            Ok(listing) => listing,
            Err(error) => return Self::meta_rejection(z2VYSE::z2VMiY, &error.to_string()),
        };
        if !listing
            .payload()
            .iter()
            .any(|registered| registered.payload() == &store)
        {
            return Self::meta_rejection(z2VYSE::z2VLwk, "store is not registered");
        }
        match self.store.retire_store(&store) {
            Ok(()) => z2VUH6::z2VdCD(meta_signal_mirror::z2Vcin::new(store)),
            Err(error) => Self::meta_rejection(z2VYSE::z2VMiY, &error.to_string()),
        }
    }

    fn set_retention(&mut self, order: meta_signal_mirror::z2VXLU) -> z2VUH6 {
        let receipt = meta_signal_mirror::z2VP3W {
            field_0: order.field_0.clone(),
            field_1: order.field_1.clone(),
        };
        match self.store.persist_retention(&order) {
            Ok(()) => z2VUH6::z2Vedh(receipt),
            Err(error) => Self::meta_rejection(z2VYSE::z2VMiY, &error.to_string()),
        }
    }

    fn observe_registry(&self) -> z2VUH6 {
        self.store
            .load_registry()
            .map(z2VUH6::z2VN4H)
            .unwrap_or_else(|error| Self::meta_rejection(z2VYSE::z2VMiY, &error.to_string()))
    }

    fn meta_rejection(reason: z2VYSE, detail: &str) -> z2VUH6 {
        z2VUH6::z2VXCa(z2Vbcq {
            field_0: reason,
            field_1: z2VbDA::new(detail.to_owned()),
        })
    }

    fn fault(error: impl std::fmt::Display) -> z2VTqL {
        z2VTqL::z2VPpj(z2Vc3D::new(z2VMCw::new(error.to_string())))
    }
}
