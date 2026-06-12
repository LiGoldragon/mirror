//! The mirror runtime engine — the data-bearing noun the schema-emitted
//! planes attach to.
//!
//! `MirrorEngine` owns the durable `Store` (the mirror's own versioned
//! sema-engine database) and implements the generated `NexusEngine` and
//! `SemaEngine` traits. One working request flows
//! Signal -> Nexus decide -> SEMA check (read) -> Nexus decision
//! (`AppendDecision` / `CheckpointDecision`, schema/nexus.schema) ->
//! SEMA persist (write) -> Signal reply. The reply leaves only after
//! the persisting redb transaction committed — ack after durable write.
//!
//! The meta tier (owner-only Unix socket) drives the same single-writer
//! SEMA plane through the DECLARED plane verbs (`schema/sema.schema`):
//! the registry read backs the pre-checks, and registration,
//! retirement, and retention are SEMA writes — the schema is the one
//! source of truth for every plane operation. Meta orders never ride
//! the working signal.

use meta_signal_mirror::{
    ConfigurationReceipt, OrderRejection, OrderRejectionReason, RejectionDetail,
};
use signal_mirror::{FaultDetail, FaultReport, Output};

use crate::config::Configuration;
use crate::error::Result;
use crate::schema::nexus::{
    self as nexus_schema, AppendDecision, CheckpointDecision, NexusAction, NexusEngine, NexusWork,
};
use crate::schema::sema::{
    self as sema_schema, LedgerFault, ReadInput, ReadOutput, SemaEngine, WriteInput, WriteOutput,
};
use crate::store::Store;

/// The mirror daemon's engine: the single writer over the mirror's own
/// versioned store.
pub struct MirrorEngine {
    store: Store,
}

impl MirrorEngine {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    /// Open the durable store at the configured path and build the
    /// engine around it.
    pub fn open(configuration: &Configuration) -> Result<Self> {
        Ok(Self::new(Store::open(configuration.storage_path())?))
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Run one decoded working `Input` end to end and return the
    /// `Output`. The execute loop owns the
    /// decide -> read -> decide -> write -> reply recursion.
    pub async fn handle(&mut self, input: signal_mirror::Input) -> Output {
        let work = NexusWork::signal_arrived(input).with_origin_route(Self::forward_origin_route());
        let action = self.execute(work).await.into_root();
        match action {
            NexusAction::ReplyToSignal(output) => output.into_payload(),
            other => Output::MirrorFaulted(FaultReport::new(FaultDetail::new(format!(
                "nexus runner returned non-reply action: {other:?}"
            )))),
        }
    }

    /// One meta order against the same single-writer plane. Meta orders
    /// are owner-only by transport (the Unix meta socket); they never
    /// arrive over TCP.
    pub fn handle_meta(&mut self, input: meta_signal_mirror::Input) -> meta_signal_mirror::Output {
        match input {
            meta_signal_mirror::Input::Configure(configure) => {
                // Binding addresses are startup facts (the binary rkyv
                // argument); the meta Configure echoes the active shape
                // back as a typed receipt. Rebinding live listeners is
                // not supported in this cut.
                meta_signal_mirror::Output::Configured(ConfigurationReceipt::new(
                    configure.into_payload(),
                ))
            }
            meta_signal_mirror::Input::RegisterStore(registration) => {
                self.register_store(registration.into_payload())
            }
            meta_signal_mirror::Input::RetireStore(retirement) => {
                self.retire_store(retirement.into_payload())
            }
            meta_signal_mirror::Input::SetRetention(order) => self.set_retention(order),
            meta_signal_mirror::Input::ObserveRegistry(_) => self.observe_registry(),
        }
    }

    fn register_store(
        &mut self,
        store: meta_signal_mirror::StoreName,
    ) -> meta_signal_mirror::Output {
        let working_name = signal_mirror::StoreName::new(store.as_str().to_owned());
        if !Store::name_is_keyable(&working_name) {
            return Self::meta_rejection(
                OrderRejectionReason::StoreNameInvalid,
                "store name carries the key separator '/'",
            );
        }
        let listing = match self.load_registered() {
            Ok(listing) => listing,
            Err(rejection) => return rejection,
        };
        if Self::registry_holds(&listing, &store) {
            return Self::meta_rejection(
                OrderRejectionReason::StoreAlreadyRegistered,
                "store is already registered",
            );
        }
        match self.apply_meta(WriteInput::RegisterStore(
            meta_signal_mirror::StoreRegistration::new(store),
        )) {
            WriteOutput::StoreRegistered(receipt) => {
                meta_signal_mirror::Output::StoreRegistered(receipt)
            }
            other => Self::meta_write_unexpected(other),
        }
    }

    fn retire_store(&mut self, store: meta_signal_mirror::StoreName) -> meta_signal_mirror::Output {
        let listing = match self.load_registered() {
            Ok(listing) => listing,
            Err(rejection) => return rejection,
        };
        if !Self::registry_holds(&listing, &store) {
            return Self::meta_rejection(
                OrderRejectionReason::StoreUnknown,
                "store is not registered",
            );
        }
        match self.apply_meta(WriteInput::RetireStore(
            meta_signal_mirror::StoreRetirement::new(store),
        )) {
            WriteOutput::StoreRetired(receipt) => meta_signal_mirror::Output::StoreRetired(receipt),
            other => Self::meta_write_unexpected(other),
        }
    }

    fn set_retention(
        &mut self,
        order: meta_signal_mirror::RetentionOrder,
    ) -> meta_signal_mirror::Output {
        match self.apply_meta(WriteInput::PersistRetention(order)) {
            WriteOutput::RetentionPersisted(receipt) => {
                meta_signal_mirror::Output::RetentionSet(receipt)
            }
            other => Self::meta_write_unexpected(other),
        }
    }

    fn observe_registry(&self) -> meta_signal_mirror::Output {
        match self.load_registered() {
            Ok(listing) => meta_signal_mirror::Output::RegistryObserved(listing),
            Err(rejection) => rejection,
        }
    }

    /// One meta write through the declared SEMA write plane.
    fn apply_meta(&mut self, input: WriteInput) -> WriteOutput {
        self.apply(sema_schema::sema::Sema::new(
            Self::meta_origin_route(),
            input,
        ))
        .into_root()
    }

    /// The registered-store listing through the declared SEMA read
    /// plane; a fault projects into the typed meta rejection.
    fn load_registered(
        &self,
    ) -> std::result::Result<meta_signal_mirror::RegistryListing, meta_signal_mirror::Output> {
        let output = self
            .observe(sema_schema::sema::Sema::new(
                Self::meta_origin_route(),
                ReadInput::LoadRegistry(meta_signal_mirror::RegistryQuery {}),
            ))
            .into_root();
        match output {
            ReadOutput::RegistryLoaded(listing) => Ok(listing),
            ReadOutput::ReadFaulted(fault) => Err(Self::meta_rejection(
                OrderRejectionReason::LedgerFault,
                fault.payload(),
            )),
            other => Err(Self::meta_rejection(
                OrderRejectionReason::LedgerFault,
                &format!("registry read returned an unexpected output: {other:?}"),
            )),
        }
    }

    fn registry_holds(
        listing: &meta_signal_mirror::RegistryListing,
        store: &meta_signal_mirror::StoreName,
    ) -> bool {
        listing
            .payload()
            .iter()
            .any(|registered| registered.payload() == store)
    }

    fn meta_write_unexpected(output: WriteOutput) -> meta_signal_mirror::Output {
        match output {
            WriteOutput::WriteFaulted(fault) => {
                Self::meta_rejection(OrderRejectionReason::LedgerFault, fault.payload())
            }
            other => Self::meta_rejection(
                OrderRejectionReason::LedgerFault,
                &format!("meta write returned an unexpected output: {other:?}"),
            ),
        }
    }

    fn meta_rejection(reason: OrderRejectionReason, detail: &str) -> meta_signal_mirror::Output {
        meta_signal_mirror::Output::OrderRejected(OrderRejection {
            reason,
            detail: RejectionDetail::new(detail.to_owned()),
        })
    }

    /// The origin route stamped onto meta-borne SEMA traffic, distinct
    /// from the working route so traces tell the planes apart. Meta
    /// orders are served one per ask on the engine actor's own call
    /// stack, so there is no concurrent in-flight mail to disambiguate.
    fn meta_origin_route() -> sema_schema::OriginRoute {
        sema_schema::OriginRoute::new(2)
    }

    /// The decision for an arrived working `Input`: every state-touching
    /// operation first loads its ledger state through the SEMA read
    /// plane.
    fn decide_signal(&self, input: signal_mirror::Input) -> NexusAction {
        match input {
            signal_mirror::Input::Append(suffix) => {
                NexusAction::command_sema_read(ReadInput::CheckAppend(suffix))
            }
            signal_mirror::Input::PublishCheckpoint(artifact) => {
                NexusAction::command_sema_read(ReadInput::CheckCheckpoint(artifact))
            }
            signal_mirror::Input::Restore(query) => {
                NexusAction::command_sema_read(ReadInput::LoadRestore(query))
            }
            signal_mirror::Input::ObserveHeads(query) => {
                NexusAction::command_sema_read(ReadInput::LoadHeads(query))
            }
        }
    }

    /// The decision for a completed SEMA read: project checked state
    /// into the schema-declared decisions, or reply directly.
    fn decide_read_completed(&self, output: ReadOutput) -> NexusAction {
        match output {
            ReadOutput::AppendChecked(checked) => match checked.into_decision() {
                AppendDecision::AcceptSuffix(novel) => {
                    NexusAction::command_sema_write(WriteInput::PersistSuffix(novel))
                }
                AppendDecision::AcknowledgeDuplicate(receipt) => {
                    NexusAction::reply_to_signal(Output::Appended(receipt))
                }
                AppendDecision::RefuseAppend(rejection) => {
                    NexusAction::reply_to_signal(Output::AppendRejected(rejection))
                }
            },
            ReadOutput::CheckpointChecked(checked) => match checked.into_decision() {
                CheckpointDecision::AcceptCheckpoint(artifact) => {
                    NexusAction::command_sema_write(WriteInput::PersistCheckpoint(artifact))
                }
                CheckpointDecision::AcknowledgeCheckpoint(receipt) => {
                    NexusAction::reply_to_signal(Output::CheckpointPublished(receipt))
                }
                CheckpointDecision::RefuseCheckpoint(rejection) => {
                    NexusAction::reply_to_signal(Output::PublishRejected(rejection))
                }
            },
            ReadOutput::RestoreLoaded(bundle) => {
                NexusAction::reply_to_signal(Output::Restored(bundle))
            }
            ReadOutput::RestoreRefused(rejection) => {
                NexusAction::reply_to_signal(Output::RestoreRejected(rejection))
            }
            ReadOutput::HeadsLoaded(listing) => {
                NexusAction::reply_to_signal(Output::HeadsObserved(listing))
            }
            ReadOutput::RegistryLoaded(_) => NexusAction::reply_to_signal(Self::faulted(
                "registry observation arrived on the working plane",
            )),
            ReadOutput::ReadFaulted(fault) => {
                NexusAction::reply_to_signal(Self::faulted(fault.payload()))
            }
        }
    }

    /// The decision for a completed SEMA write: the persisted receipt is
    /// the reply — the write transaction committed before this point.
    fn decide_write_completed(&self, output: WriteOutput) -> NexusAction {
        match output {
            WriteOutput::SuffixPersisted(receipt) => {
                NexusAction::reply_to_signal(Output::Appended(receipt))
            }
            WriteOutput::CheckpointPersisted(receipt) => {
                NexusAction::reply_to_signal(Output::CheckpointPublished(receipt))
            }
            WriteOutput::StoreRegistered(_)
            | WriteOutput::StoreRetired(_)
            | WriteOutput::RetentionPersisted(_) => NexusAction::reply_to_signal(Self::faulted(
                "meta write receipt arrived on the working plane",
            )),
            WriteOutput::WriteFaulted(fault) => {
                NexusAction::reply_to_signal(Self::faulted(fault.payload()))
            }
        }
    }

    fn faulted(detail: &str) -> Output {
        Output::MirrorFaulted(FaultReport::new(FaultDetail::new(detail.to_owned())))
    }

    fn budget_exhausted_reply(&self, exhausted: triad_runtime::ContinuationExhausted) -> Output {
        Self::faulted(&format!(
            "nexus continuation budget exhausted after {} steps (limit {})",
            exhausted.completed_step_count(),
            exhausted.limit().count()
        ))
    }

    /// The single origin route the mirror stamps onto in-flight mail.
    /// The engine actor serves one request per ask on its own call
    /// stack, so there is no concurrent in-flight mail to disambiguate.
    fn forward_origin_route() -> nexus_schema::OriginRoute {
        nexus_schema::OriginRoute::new(1)
    }

    fn sema_origin_route(origin_route: nexus_schema::OriginRoute) -> sema_schema::OriginRoute {
        sema_schema::OriginRoute::new(origin_route.payload())
    }
}

impl NexusEngine for MirrorEngine {
    fn decide(
        &mut self,
        input: nexus_schema::nexus::Nexus<nexus_schema::nexus::Work>,
    ) -> nexus_schema::nexus::Nexus<nexus_schema::nexus::Action> {
        let origin_route = input.origin_route();
        let action = match input.into_root() {
            NexusWork::SignalArrived(signal_input) => {
                self.decide_signal(signal_input.into_payload())
            }
            NexusWork::SemaReadCompleted(read) => self.decide_read_completed(read.into_payload()),
            NexusWork::SemaWriteCompleted(write) => {
                self.decide_write_completed(write.into_payload())
            }
        };
        action.with_origin_route(origin_route)
    }

    async fn execute(
        &mut self,
        input: nexus_schema::nexus::Nexus<nexus_schema::nexus::Work>,
    ) -> nexus_schema::nexus::Nexus<nexus_schema::nexus::Action> {
        let origin_route = input.origin_route();
        let mut work = input;
        let mut budget = triad_runtime::ContinuationLimit::default().budget();
        loop {
            if let Err(exhausted) = budget.spend_next_step() {
                return NexusAction::reply_to_signal(self.budget_exhausted_reply(exhausted))
                    .with_origin_route(origin_route);
            }
            self.trace_nexus_entered();
            let action = self.decide(work).into_root();
            self.trace_nexus_decided();
            match action {
                NexusAction::ReplyToSignal(_) => {
                    return action.with_origin_route(origin_route);
                }
                NexusAction::CommandSemaRead(read) => {
                    let output = self.observe(sema_schema::sema::Sema::new(
                        Self::sema_origin_route(origin_route),
                        read.into_payload(),
                    ));
                    work = NexusWork::sema_read_completed(output.into_root())
                        .with_origin_route(origin_route);
                }
                NexusAction::CommandSemaWrite(write) => {
                    let output = self.apply(sema_schema::sema::Sema::new(
                        Self::sema_origin_route(origin_route),
                        write.into_payload(),
                    ));
                    work = NexusWork::sema_write_completed(output.into_root())
                        .with_origin_route(origin_route);
                }
                NexusAction::Continue(continuation) => {
                    work = continuation.into_payload().with_origin_route(origin_route);
                }
            }
        }
    }
}

impl SemaEngine for MirrorEngine {
    fn apply_inner(
        &mut self,
        input: sema_schema::sema::Sema<WriteInput>,
    ) -> sema_schema::sema::Sema<WriteOutput> {
        let origin_route = input.origin_route();
        let output = match input.into_root() {
            WriteInput::PersistSuffix(novel) => self
                .store
                .persist_suffix(&novel)
                .map(WriteOutput::SuffixPersisted)
                .unwrap_or_else(WriteOutput::from_fault),
            WriteInput::PersistCheckpoint(artifact) => self
                .store
                .persist_checkpoint(&artifact)
                .map(WriteOutput::CheckpointPersisted)
                .unwrap_or_else(WriteOutput::from_fault),
            WriteInput::RegisterStore(registration) => {
                let store = registration.payload().clone();
                self.store
                    .register_store(&signal_mirror::StoreName::new(store.as_str().to_owned()))
                    .map(|()| {
                        WriteOutput::StoreRegistered(meta_signal_mirror::RegistrationReceipt::new(
                            store,
                        ))
                    })
                    .unwrap_or_else(WriteOutput::from_fault)
            }
            WriteInput::RetireStore(retirement) => {
                let store = retirement.payload().clone();
                self.store
                    .retire_store(&signal_mirror::StoreName::new(store.as_str().to_owned()))
                    .map(|()| {
                        WriteOutput::StoreRetired(meta_signal_mirror::RetirementReceipt::new(store))
                    })
                    .unwrap_or_else(WriteOutput::from_fault)
            }
            WriteInput::PersistRetention(order) => self
                .store
                .persist_retention(&order)
                .map(|()| {
                    WriteOutput::RetentionPersisted(meta_signal_mirror::RetentionReceipt {
                        scope: order.scope,
                        rule: order.rule,
                    })
                })
                .unwrap_or_else(WriteOutput::from_fault),
        };
        sema_schema::sema::Sema::new(origin_route, output)
    }

    fn observe_inner(
        &self,
        input: sema_schema::sema::Sema<ReadInput>,
    ) -> sema_schema::sema::Sema<ReadOutput> {
        let origin_route = input.origin_route();
        let output = match input.into_root() {
            ReadInput::CheckAppend(request) => self
                .store
                .check_append(request)
                .map(ReadOutput::AppendChecked)
                .unwrap_or_else(ReadOutput::from_fault),
            ReadInput::CheckCheckpoint(artifact) => self
                .store
                .check_checkpoint(artifact)
                .map(ReadOutput::CheckpointChecked)
                .unwrap_or_else(ReadOutput::from_fault),
            ReadInput::LoadRestore(query) => match self.store.load_restore(&query) {
                Ok(Ok(bundle)) => ReadOutput::RestoreLoaded(bundle),
                Ok(Err(rejection)) => ReadOutput::RestoreRefused(rejection),
                Err(error) => ReadOutput::from_fault(error),
            },
            ReadInput::LoadHeads(query) => self
                .store
                .load_heads(&query)
                .map(ReadOutput::HeadsLoaded)
                .unwrap_or_else(ReadOutput::from_fault),
            ReadInput::LoadRegistry(_) => self
                .store
                .load_registry()
                .map(ReadOutput::RegistryLoaded)
                .unwrap_or_else(ReadOutput::from_fault),
        };
        sema_schema::sema::Sema::new(origin_route, output)
    }
}

impl WriteOutput {
    fn from_fault(error: crate::error::Error) -> Self {
        Self::WriteFaulted(LedgerFault::new(error.to_string()))
    }
}

impl ReadOutput {
    fn from_fault(error: crate::error::Error) -> Self {
        Self::ReadFaulted(LedgerFault::new(error.to_string()))
    }
}
