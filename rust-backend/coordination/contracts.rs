use super::domain::{
    Claim, ClaimId, ClaimState, ContractEventPayload, ContractExpectation, ContractId,
    ContractKind, CoordinationActor, CoordinationEvent, CoordinationEventPayload, DomainError,
    EventSeverity, SharedContract, WorkPackage, WorkPackageId, WorkerId,
};
use super::store::{
    ClaimRepository, ContractRepository, SqliteCoordinationStore, StoreError, WorkPackageRepository,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct ContractRegistration {
    pub goal_id: String,
    pub stable_key: String,
    pub title: String,
    pub kind: ContractKind,
    pub producer: Option<WorkPackageId>,
    pub compatibility_notes: String,
}

#[derive(Debug, Clone, Default)]
pub struct ContractDependencyDeclaration {
    pub produces: Vec<String>,
    pub consumes: Vec<ContractExpectation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractRevisionMismatch {
    pub claim_id: ClaimId,
    pub worker_id: WorkerId,
    pub contract_id: ContractId,
    pub stable_key: String,
    pub expected_revision: u64,
    pub current_revision: u64,
}

#[derive(Debug, Error)]
pub enum ContractRegistryError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("contract registration field is invalid: {0}")]
    InvalidRegistration(&'static str),
    #[error("shared contract was not found: {0}")]
    NotFound(String),
    #[error("expected contract revision {expected}, but current revision is {current}")]
    RevisionMismatch { expected: u64, current: u64 },
    #[error("work package was not found: {0}")]
    WorkPackageNotFound(String),
    #[error("claim was not found: {0}")]
    ClaimNotFound(String),
    #[error("claim is not active or is owned by another worker")]
    ClaimNotOwned,
    #[error("contract dependency is invalid: {0}")]
    InvalidDependency(String),
    #[error("contract dependencies changed concurrently")]
    StaleDeclaration,
    #[error("event actor does not match the attributed contract worker")]
    ActorMismatch,
}

#[derive(Debug, Clone)]
pub struct ContractRegistry {
    store: Arc<SqliteCoordinationStore>,
}

impl ContractRegistry {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self { store }
    }

    pub fn register(
        &self,
        registration: ContractRegistration,
        actor: CoordinationActor,
        changed_by: Option<WorkerId>,
        now: DateTime<Utc>,
    ) -> Result<SharedContract, ContractRegistryError> {
        validate_registration(&registration)?;
        validate_contract_actor(&actor, changed_by.as_ref())?;
        let mut contract = SharedContract::new(
            &registration.goal_id,
            &registration.stable_key,
            &registration.title,
            registration.kind,
            now,
        );
        contract.producer = registration.producer;
        contract.compatibility_notes = registration.compatibility_notes;
        contract.last_changed_by = changed_by.clone();
        let event = CoordinationEvent::from_typed_payload(
            &contract.goal_id,
            EventSeverity::Info,
            actor,
            format!("contract:{}", contract.stable_key),
            CoordinationEventPayload::Contract(ContractEventPayload {
                contract_id: contract.id.clone(),
                stable_key: contract.stable_key.clone(),
                previous_revision: 0,
                revision: contract.revision,
                producer_worker_id: changed_by,
                compatibility_notes: contract.compatibility_notes.clone(),
            }),
            now,
        )?;
        self.store.register_contract_with_event(
            &contract,
            &event,
            &format!(
                "contract.register:{}:{}",
                contract.goal_id, contract.stable_key
            ),
        )?;
        Ok(contract)
    }

    pub fn revise(
        &self,
        contract_id: &ContractId,
        expected_revision: u64,
        changed_by: WorkerId,
        compatibility_notes: String,
        actor: CoordinationActor,
        now: DateTime<Utc>,
    ) -> Result<SharedContract, ContractRegistryError> {
        validate_text(&compatibility_notes, "compatibility notes", 2_000)?;
        validate_contract_actor(&actor, Some(&changed_by))?;
        let mut contract = self
            .store
            .contract(contract_id)?
            .ok_or_else(|| ContractRegistryError::NotFound(contract_id.as_str().into()))?;
        if contract.revision != expected_revision {
            return Err(ContractRegistryError::RevisionMismatch {
                expected: expected_revision,
                current: contract.revision,
            });
        }
        contract.revise(changed_by.clone(), compatibility_notes.clone(), now);
        let event = CoordinationEvent::from_typed_payload(
            &contract.goal_id,
            EventSeverity::Warning,
            actor,
            format!("contract:{}", contract.stable_key),
            CoordinationEventPayload::Contract(ContractEventPayload {
                contract_id: contract.id.clone(),
                stable_key: contract.stable_key.clone(),
                previous_revision: expected_revision,
                revision: contract.revision,
                producer_worker_id: Some(changed_by),
                compatibility_notes,
            }),
            now,
        )?;
        if self
            .store
            .revise_contract_with_event(
                &contract,
                expected_revision,
                &event,
                &format!(
                    "contract.revise:{}:{}",
                    contract.id.as_str(),
                    contract.revision
                ),
            )?
            .is_none()
        {
            let current = self
                .store
                .contract(contract_id)?
                .map(|current| current.revision)
                .unwrap_or_default();
            return Err(ContractRegistryError::RevisionMismatch {
                expected: expected_revision,
                current,
            });
        }
        Ok(contract)
    }

    pub fn get(&self, contract_id: &ContractId) -> Result<SharedContract, ContractRegistryError> {
        self.store
            .contract(contract_id)?
            .ok_or_else(|| ContractRegistryError::NotFound(contract_id.as_str().into()))
    }

    pub fn get_by_stable_key(
        &self,
        goal_id: &str,
        stable_key: &str,
    ) -> Result<Option<SharedContract>, ContractRegistryError> {
        Ok(self.store.contract_by_stable_key(goal_id, stable_key)?)
    }

    pub fn list(&self, goal_id: &str) -> Result<Vec<SharedContract>, ContractRegistryError> {
        Ok(self.store.contracts_for_goal(goal_id)?)
    }

    pub fn declare_for_work_package(
        &self,
        package_id: &WorkPackageId,
        declaration: ContractDependencyDeclaration,
        now: DateTime<Utc>,
    ) -> Result<WorkPackage, ContractRegistryError> {
        let mut package = self.store.work_package(package_id)?.ok_or_else(|| {
            ContractRegistryError::WorkPackageNotFound(package_id.as_str().into())
        })?;
        let (produces, consumes) = self.resolve_dependencies(&package.goal_id, declaration)?;
        let expected_updated_at = package.metadata.updated_at;
        package.produces_contracts = produces;
        package.consumes_contracts = consumes;
        package
            .metadata
            .touch(monotonic_time(now, expected_updated_at));
        if !self
            .store
            .update_work_package_contracts(&package, expected_updated_at)?
        {
            return Err(ContractRegistryError::StaleDeclaration);
        }
        Ok(package)
    }

    pub fn declare_for_claim(
        &self,
        claim_id: &ClaimId,
        worker_id: &WorkerId,
        expected_generation: u64,
        declaration: ContractDependencyDeclaration,
        now: DateTime<Utc>,
    ) -> Result<Claim, ContractRegistryError> {
        let mut claim = self
            .store
            .claim(claim_id)?
            .ok_or_else(|| ContractRegistryError::ClaimNotFound(claim_id.as_str().into()))?;
        if claim.state != ClaimState::Active
            || claim.owner != *worker_id
            || claim.lease_generation != expected_generation
        {
            return Err(ContractRegistryError::ClaimNotOwned);
        }
        let (produces, consumes) = self.resolve_dependencies(&claim.goal_id, declaration)?;
        let expected_updated_at = claim.metadata.updated_at;
        claim.produces_contracts = produces;
        claim.consumes_contracts = consumes;
        claim
            .metadata
            .touch(monotonic_time(now, expected_updated_at));
        if !self.store.update_active_claim_contracts(
            &claim,
            expected_generation,
            expected_updated_at,
        )? {
            return Err(ContractRegistryError::StaleDeclaration);
        }
        Ok(claim)
    }

    pub fn revision_mismatches_for_goal(
        &self,
        goal_id: &str,
    ) -> Result<Vec<ContractRevisionMismatch>, ContractRegistryError> {
        let contracts = self.store.contracts_for_goal(goal_id)?;
        let mut lookup = HashMap::new();
        for contract in &contracts {
            lookup.insert(contract.id.as_str(), contract);
            lookup.insert(contract.stable_key.as_str(), contract);
        }
        let mut mismatches = Vec::new();
        for claim in self
            .store
            .claims_for_goal(goal_id, Some(ClaimState::Active))?
        {
            for expectation in &claim.consumes_contracts {
                let Some(contract) = lookup.get(expectation.contract_id.as_str()) else {
                    continue;
                };
                if expectation.expected_revision != contract.revision {
                    mismatches.push(ContractRevisionMismatch {
                        claim_id: claim.id.clone(),
                        worker_id: claim.owner.clone(),
                        contract_id: contract.id.clone(),
                        stable_key: contract.stable_key.clone(),
                        expected_revision: expectation.expected_revision,
                        current_revision: contract.revision,
                    });
                }
            }
        }
        mismatches.sort_by(|left, right| {
            left.worker_id
                .as_str()
                .cmp(right.worker_id.as_str())
                .then_with(|| left.claim_id.as_str().cmp(right.claim_id.as_str()))
                .then_with(|| left.contract_id.as_str().cmp(right.contract_id.as_str()))
        });
        Ok(mismatches)
    }

    pub fn revision_mismatches_for_claim(
        &self,
        claim_id: &ClaimId,
    ) -> Result<Vec<ContractRevisionMismatch>, ContractRegistryError> {
        let claim = self
            .store
            .claim(claim_id)?
            .ok_or_else(|| ContractRegistryError::ClaimNotFound(claim_id.as_str().into()))?;
        Ok(self
            .revision_mismatches_for_goal(&claim.goal_id)?
            .into_iter()
            .filter(|mismatch| mismatch.claim_id == *claim_id)
            .collect())
    }

    fn resolve_dependencies(
        &self,
        goal_id: &str,
        declaration: ContractDependencyDeclaration,
    ) -> Result<(Vec<String>, Vec<ContractExpectation>), ContractRegistryError> {
        let contracts = self.store.contracts_for_goal(goal_id)?;
        let mut lookup = HashMap::new();
        for contract in &contracts {
            lookup.insert(contract.id.as_str(), contract.id.as_str());
            lookup.insert(contract.stable_key.as_str(), contract.id.as_str());
        }
        let mut produced = Vec::new();
        let mut produced_set = HashSet::new();
        for reference in declaration.produces {
            let id = lookup.get(reference.as_str()).ok_or_else(|| {
                ContractRegistryError::InvalidDependency(format!(
                    "unknown produced contract {reference}"
                ))
            })?;
            if !produced_set.insert((*id).to_string()) {
                return Err(ContractRegistryError::InvalidDependency(format!(
                    "duplicate produced contract {reference}"
                )));
            }
            produced.push((*id).to_string());
        }
        let mut consumed = Vec::new();
        let mut consumed_set = HashSet::new();
        for expectation in declaration.consumes {
            if expectation.expected_revision == 0 {
                return Err(ContractRegistryError::InvalidDependency(
                    "expected revision must be greater than zero".into(),
                ));
            }
            let id = lookup
                .get(expectation.contract_id.as_str())
                .ok_or_else(|| {
                    ContractRegistryError::InvalidDependency(format!(
                        "unknown consumed contract {}",
                        expectation.contract_id
                    ))
                })?;
            if produced_set.contains(*id) {
                return Err(ContractRegistryError::InvalidDependency(format!(
                    "contract {id} cannot be both produced and consumed"
                )));
            }
            if !consumed_set.insert((*id).to_string()) {
                return Err(ContractRegistryError::InvalidDependency(format!(
                    "duplicate consumed contract {}",
                    expectation.contract_id
                )));
            }
            consumed.push(ContractExpectation {
                contract_id: (*id).to_string(),
                expected_revision: expectation.expected_revision,
            });
        }
        produced.sort();
        consumed.sort_by(|left, right| left.contract_id.cmp(&right.contract_id));
        Ok((produced, consumed))
    }
}

fn monotonic_time(now: DateTime<Utc>, previous: DateTime<Utc>) -> DateTime<Utc> {
    if now > previous {
        now
    } else {
        previous + Duration::nanoseconds(1)
    }
}

fn validate_contract_actor(
    actor: &CoordinationActor,
    changed_by: Option<&WorkerId>,
) -> Result<(), ContractRegistryError> {
    if let CoordinationActor::Worker { worker_id } = actor {
        if changed_by != Some(worker_id) {
            return Err(ContractRegistryError::ActorMismatch);
        }
    }
    Ok(())
}

fn validate_registration(registration: &ContractRegistration) -> Result<(), ContractRegistryError> {
    validate_text(&registration.goal_id, "goal id", 200)?;
    validate_text(&registration.title, "title", 200)?;
    validate_text(
        &registration.compatibility_notes,
        "compatibility notes",
        2_000,
    )?;
    if registration.stable_key.is_empty()
        || registration.stable_key.len() > 160
        || !registration.stable_key.chars().all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '.' | '_' | '-' | ':' | '/')
        })
    {
        return Err(ContractRegistryError::InvalidRegistration("stable key"));
    }
    Ok(())
}

fn validate_text(
    value: &str,
    field: &'static str,
    max_chars: usize,
) -> Result<(), ContractRegistryError> {
    if value.trim().is_empty() || value.chars().count() > max_chars {
        Err(ContractRegistryError::InvalidRegistration(field))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::domain::{ContractKind, CoordinationEventKind};
    use crate::coordination::store::EventRepository;

    fn registry() -> (
        tempfile::TempDir,
        Arc<SqliteCoordinationStore>,
        ContractRegistry,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let registry = ContractRegistry::new(Arc::clone(&store));
        (directory, store, registry)
    }

    fn registration(kind: ContractKind) -> ContractRegistration {
        ContractRegistration {
            goal_id: "goal-a".into(),
            stable_key: "api.worker.v1".into(),
            title: "Worker API".into(),
            kind,
            producer: Some(WorkPackageId::new()),
            compatibility_notes: "Initial compatible contract".into(),
        }
    }

    #[test]
    fn registers_every_supported_contract_kind_at_revision_one() {
        let (_directory, _store, registry) = registry();
        let now = Utc::now();
        let kinds = [
            ContractKind::Api,
            ContractKind::PersistedSchema,
            ContractKind::Migration,
            ContractKind::DomainModel,
            ContractKind::Configuration,
            ContractKind::GeneratedInterface,
            ContractKind::Serialization,
        ];
        for (index, kind) in kinds.into_iter().enumerate() {
            let mut request = registration(kind.clone());
            request.stable_key = format!("contract.{index}");
            let contract = registry
                .register(request, CoordinationActor::System, None, now)
                .unwrap();
            assert_eq!(contract.kind, kind);
            assert_eq!(contract.revision, 1);
        }
        assert_eq!(registry.list("goal-a").unwrap().len(), 7);
        let first = registry
            .get_by_stable_key("goal-a", "contract.0")
            .unwrap()
            .unwrap();
        assert_eq!(registry.get(&first.id).unwrap(), first);
    }

    #[test]
    fn rejects_invalid_or_duplicate_stable_keys() {
        let (_directory, _store, registry) = registry();
        let now = Utc::now();
        let mut invalid = registration(ContractKind::Api);
        invalid.stable_key = "API Has Spaces".into();
        assert!(matches!(
            registry.register(invalid, CoordinationActor::System, None, now),
            Err(ContractRegistryError::InvalidRegistration("stable key"))
        ));

        registry
            .register(
                registration(ContractKind::Api),
                CoordinationActor::System,
                None,
                now,
            )
            .unwrap();
        assert!(matches!(
            registry.register(
                registration(ContractKind::DomainModel),
                CoordinationActor::System,
                None,
                now,
            ),
            Err(ContractRegistryError::Store(
                StoreError::ContractAlreadyExists { .. }
            ))
        ));

        assert!(matches!(
            registry.register(
                {
                    let mut request = registration(ContractKind::Api);
                    request.stable_key = "api.actor-mismatch".into();
                    request
                },
                CoordinationActor::Worker {
                    worker_id: WorkerId::new(),
                },
                Some(WorkerId::new()),
                now,
            ),
            Err(ContractRegistryError::ActorMismatch)
        ));
    }

    #[test]
    fn revisions_are_monotonic_attributed_and_evented() {
        let (_directory, store, registry) = registry();
        let now = Utc::now();
        let contract = registry
            .register(
                registration(ContractKind::Api),
                CoordinationActor::System,
                None,
                now,
            )
            .unwrap();
        let worker = WorkerId::new();
        let revised = registry
            .revise(
                &contract.id,
                1,
                worker.clone(),
                "Adds an optional field".into(),
                CoordinationActor::Worker {
                    worker_id: worker.clone(),
                },
                now,
            )
            .unwrap();

        assert_eq!(revised.revision, 2);
        assert_eq!(revised.last_changed_by, Some(worker));
        assert!(matches!(
            registry.revise(
                &contract.id,
                1,
                WorkerId::new(),
                "stale".into(),
                CoordinationActor::System,
                now,
            ),
            Err(ContractRegistryError::RevisionMismatch {
                expected: 1,
                current: 2
            })
        ));
        let events = store.events_for_goal("goal-a", 0, None, 10).unwrap();
        assert_eq!(events.len(), 2);
        assert!(
            events
                .iter()
                .all(|event| event.kind == CoordinationEventKind::ContractChanged)
        );
    }

    #[test]
    fn work_packages_declare_canonical_produced_and_consumed_contracts() {
        let (_directory, store, registry) = registry();
        let now = Utc::now();
        let mut producer_registration = registration(ContractKind::Api);
        producer_registration.stable_key = "api.produced".into();
        let produced = registry
            .register(producer_registration, CoordinationActor::System, None, now)
            .unwrap();
        let mut consumer_registration = registration(ContractKind::Serialization);
        consumer_registration.stable_key = "schema.consumed".into();
        let consumed = registry
            .register(consumer_registration, CoordinationActor::System, None, now)
            .unwrap();
        let package =
            WorkPackage::new("goal-a", "API package", vec!["feature-a".into()], now).unwrap();
        store.upsert_work_package(&package).unwrap();

        let declared = registry
            .declare_for_work_package(
                &package.id,
                ContractDependencyDeclaration {
                    produces: vec![produced.stable_key.clone()],
                    consumes: vec![ContractExpectation {
                        contract_id: consumed.id.as_str().into(),
                        expected_revision: 1,
                    }],
                },
                now,
            )
            .unwrap();

        assert_eq!(declared.produces_contracts, vec![produced.id.as_str()]);
        assert_eq!(
            declared.consumes_contracts[0].contract_id,
            consumed.id.as_str()
        );
        assert_eq!(declared.consumes_contracts[0].expected_revision, 1);
        assert_eq!(store.work_package(&package.id).unwrap().unwrap(), declared);
    }

    #[test]
    fn active_claims_can_refine_dependencies_only_for_their_owner() {
        let (_directory, store, registry) = registry();
        let now = Utc::now();
        let contract = registry
            .register(
                registration(ContractKind::Api),
                CoordinationActor::System,
                None,
                now,
            )
            .unwrap();
        let worker = WorkerId::new();
        let claim = Claim::new(
            "goal-a",
            super::super::domain::ClaimScope::Feature {
                feature_id: "feature-a".into(),
            },
            worker.clone(),
            "base-a",
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        store.insert_claim(&claim).unwrap();
        let declaration = ContractDependencyDeclaration {
            produces: vec![],
            consumes: vec![ContractExpectation {
                contract_id: contract.stable_key.clone(),
                expected_revision: 1,
            }],
        };

        assert!(matches!(
            registry.declare_for_claim(
                &claim.id,
                &WorkerId::new(),
                claim.lease_generation,
                declaration.clone(),
                now,
            ),
            Err(ContractRegistryError::ClaimNotOwned)
        ));
        let declared = registry
            .declare_for_claim(&claim.id, &worker, claim.lease_generation, declaration, now)
            .unwrap();
        assert_eq!(
            declared.consumes_contracts[0].contract_id,
            contract.id.as_str()
        );
        assert_eq!(store.claim(&claim.id).unwrap().unwrap(), declared);
    }

    #[test]
    fn dependency_declarations_reject_unknown_duplicates_and_zero_revisions() {
        let (_directory, store, registry) = registry();
        let now = Utc::now();
        let contract = registry
            .register(
                registration(ContractKind::Api),
                CoordinationActor::System,
                None,
                now,
            )
            .unwrap();
        let package =
            WorkPackage::new("goal-a", "API package", vec!["feature-a".into()], now).unwrap();
        store.upsert_work_package(&package).unwrap();

        for declaration in [
            ContractDependencyDeclaration {
                produces: vec!["missing.contract".into()],
                consumes: vec![],
            },
            ContractDependencyDeclaration {
                produces: vec![contract.id.as_str().into(), contract.stable_key.clone()],
                consumes: vec![],
            },
            ContractDependencyDeclaration {
                produces: vec![],
                consumes: vec![ContractExpectation {
                    contract_id: contract.id.as_str().into(),
                    expected_revision: 0,
                }],
            },
        ] {
            assert!(matches!(
                registry.declare_for_work_package(&package.id, declaration, now),
                Err(ContractRegistryError::InvalidDependency(_))
            ));
        }
    }

    #[test]
    fn detects_active_claims_with_stale_expected_revisions() {
        let (_directory, store, registry) = registry();
        let now = Utc::now();
        let contract = registry
            .register(
                registration(ContractKind::Api),
                CoordinationActor::System,
                None,
                now,
            )
            .unwrap();
        let worker = WorkerId::new();
        let mut claim = Claim::new(
            "goal-a",
            super::super::domain::ClaimScope::Feature {
                feature_id: "feature-a".into(),
            },
            worker.clone(),
            "base-a",
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        claim.consumes_contracts.push(ContractExpectation {
            contract_id: contract.id.as_str().into(),
            expected_revision: 1,
        });
        store.insert_claim(&claim).unwrap();
        assert!(
            registry
                .revision_mismatches_for_goal("goal-a")
                .unwrap()
                .is_empty()
        );

        registry
            .revise(
                &contract.id,
                1,
                WorkerId::new(),
                "Material response shape change".into(),
                CoordinationActor::System,
                now,
            )
            .unwrap();
        let mismatches = registry.revision_mismatches_for_claim(&claim.id).unwrap();

        assert_eq!(mismatches.len(), 1);
        assert_eq!(mismatches[0].worker_id, worker);
        assert_eq!(mismatches[0].expected_revision, 1);
        assert_eq!(mismatches[0].current_revision, 2);
    }
}
