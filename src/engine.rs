//! Mirror's single-writer runtime engine.
//!
//! Every state-bearing request follows the same visible law: load durable
//! ledger state, decide purely, commit, then acknowledge. The Interface meets
//! that durable boundary directly.

use meta_signal_mirror::{
    OrderRejection, OrderRejectionReason, Query as MetaQuery, Response as MetaResponse,
    RetentionOrder, RetentionReceipt, StoreRegistration,
};
use signal_mirror::{Query, Response, StoreName};

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

    pub async fn handle(&mut self, input: Query) -> Response {
        match input {
            Query::Append(request) => match self.store.check_append(request) {
                Ok(checked) => match checked.into_decision() {
                    AppendDecision::Accept(suffix) => self
                        .store
                        .persist_suffix(&suffix)
                        .map(Response::Appended)
                        .unwrap_or_else(Self::fault),
                    AppendDecision::Duplicate(receipt) => Response::Appended(receipt),
                    AppendDecision::Refuse(rejection) => Response::AppendRejected(rejection),
                },
                Err(error) => Self::fault(error),
            },
            Query::PublishCheckpoint(artifact) => match self.store.check_checkpoint(artifact) {
                Ok(checked) => match checked.into_decision() {
                    CheckpointDecision::Accept(artifact) => self
                        .store
                        .persist_checkpoint(&artifact)
                        .map(Response::CheckpointPublished)
                        .unwrap_or_else(Self::fault),
                    CheckpointDecision::Duplicate(receipt) => {
                        Response::CheckpointPublished(receipt)
                    }
                    CheckpointDecision::Refuse(rejection) => Response::PublishRejected(rejection),
                },
                Err(error) => Self::fault(error),
            },
            Query::NotifyObject(notice) => match self.store.check_object_notice(notice) {
                Ok(checked) => match checked.into_decision() {
                    ObjectNoticeDecision::Accept(receipt) => {
                        Response::ObjectNoticeAccepted(receipt)
                    }
                    ObjectNoticeDecision::Refuse(rejection) => {
                        Response::ObjectNoticeRejected(rejection)
                    }
                },
                Err(error) => Self::fault(error),
            },
            Query::Restore(query) => match self.store.load_restore(&query) {
                Ok(Ok(bundle)) => Response::Restored(bundle),
                Ok(Err(rejection)) => Response::RestoreRejected(rejection),
                Err(error) => Self::fault(error),
            },
            Query::ObserveHeads(query) => self
                .store
                .load_heads(&query)
                .map(Response::HeadsObserved)
                .unwrap_or_else(Self::fault),
        }
    }

    pub fn handle_meta(&mut self, input: MetaQuery) -> MetaResponse {
        match input {
            MetaQuery::Configure(configuration) => MetaResponse::Configured(configuration),
            MetaQuery::RegisterStore(registration) => self.register_store(registration),
            MetaQuery::RetireStore(retirement) => self.retire_store(retirement),
            MetaQuery::SetRetention(order) => self.set_retention(order),
            MetaQuery::ObserveRegistry(_) => self.observe_registry(),
        }
    }

    fn register_store(&mut self, registration: StoreRegistration) -> MetaResponse {
        if !Store::name_is_keyable(&registration.store_name) {
            return Self::meta_rejection(
                OrderRejectionReason::StoreNameInvalid,
                "store name carries the key separator '/'",
            );
        }
        let listing = match self.store.load_registry() {
            Ok(listing) => listing,
            Err(error) => {
                return Self::meta_rejection(OrderRejectionReason::LedgerFault, &error.to_string());
            }
        };
        if listing
            .registered_store_vector
            .iter()
            .any(|registered| registered == &registration.store_name)
        {
            return Self::meta_rejection(
                OrderRejectionReason::StoreAlreadyRegistered,
                "store is already registered",
            );
        }
        let store = registration.store_name;
        let addressing = ContentAddressing::from_meta(&registration.content_addressing);
        match self.store.register_store(&store, addressing) {
            Ok(()) => MetaResponse::StoreRegistered(store),
            Err(error) => {
                Self::meta_rejection(OrderRejectionReason::LedgerFault, &error.to_string())
            }
        }
    }

    fn retire_store(&mut self, store: StoreName) -> MetaResponse {
        let listing = match self.store.load_registry() {
            Ok(listing) => listing,
            Err(error) => {
                return Self::meta_rejection(OrderRejectionReason::LedgerFault, &error.to_string());
            }
        };
        if !listing
            .registered_store_vector
            .iter()
            .any(|registered| registered == &store)
        {
            return Self::meta_rejection(
                OrderRejectionReason::StoreUnknown,
                "store is not registered",
            );
        }
        match self.store.retire_store(&store) {
            Ok(()) => MetaResponse::StoreRetired(store),
            Err(error) => {
                Self::meta_rejection(OrderRejectionReason::LedgerFault, &error.to_string())
            }
        }
    }

    fn set_retention(&mut self, order: RetentionOrder) -> MetaResponse {
        let receipt = meta_signal_mirror::RetentionReceipt {
            retention_scope: order.retention_scope.clone(),
            retention_rule: order.retention_rule.clone(),
        };
        match self.store.persist_retention(&order) {
            Ok(()) => MetaResponse::RetentionSet(receipt),
            Err(error) => {
                Self::meta_rejection(OrderRejectionReason::LedgerFault, &error.to_string())
            }
        }
    }

    fn observe_registry(&self) -> MetaResponse {
        self.store
            .load_registry()
            .map(MetaResponse::RegistryObserved)
            .unwrap_or_else(|error| {
                Self::meta_rejection(OrderRejectionReason::LedgerFault, &error.to_string())
            })
    }

    fn meta_rejection(reason: OrderRejectionReason, detail: &str) -> MetaResponse {
        MetaResponse::OrderRejected(OrderRejection {
            order_rejection_reason: reason,
            rejection_detail: detail.to_owned(),
        })
    }

    fn fault(error: impl std::fmt::Display) -> Response {
        Response::MirrorFaulted(error.to_string())
    }
}
