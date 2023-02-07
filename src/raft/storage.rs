use std::{
    collections::{BTreeMap, HashSet},
    io::Cursor,
};

use anyhow::Result;
use async_raft::{
    async_trait::async_trait,
    raft::{Entry, EntryPayload, MembershipConfig},
    storage::{CurrentSnapshotData, HardState, InitialState},
    NodeId, RaftStorage,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;

use crate::{
    api::{DropKeyQueryParams, GetKeyQueryParams, SetKeyJsonBody},
    cache::{Cache, FullType},
};

use super::{CacheRequest, CacheResponse};

/// The application snapshot type which the Storage works with.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct Snapshot {
    /// The last index covered by this snapshot.
    index: u64,
    /// The term of the last index covered by this snapshot.
    term: u64,
    /// The last memberhsip config included in this snapshot.
    membership: MembershipConfig,
    /// The data of the state machine at the time of this snapshot.
    data: Vec<u8>,
}

/// The state machine of the NetworkNode.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct StateMachine<T>
where
    T: Cache,
{
    last_applied_log: u64,
    cache: T,
}

impl<T> StateMachine<T>
where
    T: Cache,
{
    fn new(cache: T) -> Self {
        Self {
            last_applied_log: 0,
            cache,
        }
    }
}

/// Error used to trigger Raft shutdown from storage.
#[derive(Clone, Debug, Error)]
pub enum ShutdownError {
    #[error("unsafe storage error")]
    _UnsafeStorageError,
}

pub(super) struct CacheStorage<T>
where
    T: Cache,
{
    /// The ID of the Raft node for which this memory storage instances is configured.
    id: NodeId,
    /// The Raft log.
    log: RwLock<BTreeMap<u64, Entry<CacheRequest>>>,
    /// The Raft state machine.
    sm: RwLock<StateMachine<T>>,
    /// The current hard state.
    hs: RwLock<Option<HardState>>,
    /// The current snapshot.
    current_snapshot: RwLock<Option<Snapshot>>,
}

impl<T> CacheStorage<T>
where
    T: Cache + Default,
    CacheStorage<T>: Sync,
{
    pub fn new(id: NodeId, cache: T) -> Self {
        let log = RwLock::new(BTreeMap::new());
        let sm = RwLock::new(StateMachine::new(cache));
        let hs = RwLock::new(None);
        let current_snapshot = RwLock::new(None);
        Self {
            id,
            log,
            sm,
            hs,
            current_snapshot,
        }
    }

    // async fn get(&mut self, k: &KeyType) -> Option<ValueType> {
    //     self.sm.write().await.cache.get(k).await
    // }

    // async fn set(&mut self, k: &KeyType, v: ValueType, duration: SystemTime) -> () {
    //     self.sm.write().await.cache.set(k, v, duration).await
    // }

    // async fn to_iter(&mut self) -> FuturesUnordered<EntryType> {
    //     self.sm.write().await.cache.to_iter().await
    // }
}

#[async_trait]
impl<T> RaftStorage<CacheRequest, CacheResponse> for CacheStorage<T>
where
    T: Cache + Default + Send + Sync + 'static,
{
    type Snapshot = Cursor<Vec<u8>>;
    type ShutdownError = ShutdownError;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        let log = self.log.read().await;
        let cfg_opt = log.values().rev().find_map(|entry| match &entry.payload {
            EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
            EntryPayload::SnapshotPointer(snap) => Some(snap.membership.clone()),
            _ => None,
        });
        Ok(match cfg_opt {
            Some(cfg) => cfg,
            None => MembershipConfig::new_initial(self.id),
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_initial_state(&self) -> Result<InitialState> {
        let membership = self.get_membership_config().await?;
        let mut hs = self.hs.write().await;
        let log = self.log.read().await;
        let sm = self.sm.read().await;
        match &mut *hs {
            Some(inner) => {
                let (last_log_index, last_log_term) = match log.values().rev().next() {
                    Some(log) => (log.index, log.term),
                    None => (0, 0),
                };
                let last_applied_log = sm.last_applied_log;
                Ok(InitialState {
                    last_log_index,
                    last_log_term,
                    last_applied_log,
                    hard_state: inner.clone(),
                    membership,
                })
            }
            None => {
                let new = InitialState::new_initial(self.id);
                *hs = Some(new.hard_state.clone());
                Ok(new)
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self, hs))]
    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        *self.hs.write().await = Some(hs.clone());
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<CacheRequest>>> {
        // Invalid request, return empty vec.
        if start > stop {
            tracing::error!("invalid request, start > stop");
            return Ok(vec![]);
        }
        let log = self.log.read().await;
        Ok(log.range(start..stop).map(|(_, val)| val.clone()).collect())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<()> {
        if stop.as_ref().map(|stop| &start > stop).unwrap_or(false) {
            tracing::error!("invalid request, start > stop");
            return Ok(());
        }
        let mut log = self.log.write().await;

        // If a stop point was specified, delete from start until the given stop point.
        if let Some(stop) = stop.as_ref() {
            for key in start..*stop {
                log.remove(&key);
            }
            return Ok(());
        }
        // Else, just split off the remainder.
        log.split_off(&start);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, entry))]
    async fn append_entry_to_log(&self, entry: &Entry<CacheRequest>) -> Result<()> {
        let mut log = self.log.write().await;
        log.insert(entry.index, entry.clone());
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn replicate_to_log(&self, entries: &[Entry<CacheRequest>]) -> Result<()> {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.index, entry.clone());
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, data))]
    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        data: &CacheRequest,
    ) -> Result<CacheResponse> {
        let mut sm = self.sm.write().await;
        sm.last_applied_log = *index;
        match data {
            CacheRequest::GetKey(GetKeyQueryParams { key, now }) => {
                Ok(CacheResponse::GetKey(sm.cache.get(*now, key).await))
            }
            CacheRequest::SetKey(SetKeyJsonBody {
                key,
                value,
                exp_time,
            }) => {
                sm.cache.set(key, value.clone(), *exp_time).await;
                Ok(CacheResponse::SetKey())
            }
            CacheRequest::DropKey(DropKeyQueryParams { key }) => {
                sm.cache.drop(key).await;
                Ok(CacheResponse::DropKey())
            }
            CacheRequest::Iter(now) => {
                let set = sm.cache.get_all(*now).await.into_iter().collect();
                Ok(CacheResponse::Iter(set))
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn replicate_to_state_machine(&self, entries: &[(&u64, &CacheRequest)]) -> Result<()> {
        let mut sm = self.sm.write().await;
        for (index, data) in entries {
            sm.last_applied_log = **index;
            match data {
                CacheRequest::GetKey(GetKeyQueryParams { key, now }) => {
                    sm.cache.get(*now, key).await;
                }
                CacheRequest::SetKey(SetKeyJsonBody {
                    key,
                    value,
                    exp_time,
                }) => {
                    sm.cache.set(key, value.to_string(), *exp_time).await;
                }
                CacheRequest::DropKey(DropKeyQueryParams { key }) => {
                    sm.cache.drop(key).await;
                }
                CacheRequest::Iter(now) => {
                    let _: HashSet<_> = sm.cache.get_all(*now).await.into_iter().collect();
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>> {
        let (data, last_applied_log);
        {
            // Serialize the data of the state machine.
            let mut sm = self.sm.write().await;
            let entries: Vec<_> = sm.cache.get_all(u64::MAX).await.into_iter().collect();
            data = serde_json::to_vec(&(sm.last_applied_log, entries))?;
            last_applied_log = sm.last_applied_log;
        } // Release state machine read lock.

        let membership_config;
        {
            // Go backwards through the log to find the most recent membership config <= the `through` index.
            let log = self.log.read().await;
            membership_config = log
                .values()
                .rev()
                .skip_while(|entry| entry.index > last_applied_log)
                .find_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.id));
        } // Release log read lock.

        let snapshot_bytes: Vec<u8>;
        let term;
        {
            let mut log = self.log.write().await;
            let mut current_snapshot = self.current_snapshot.write().await;
            term = log
                .get(&last_applied_log)
                .map(|entry| entry.term)
                .ok_or_else(|| anyhow::anyhow!("ERR_INCONSISTENT_LOG"))?;
            *log = log.split_off(&last_applied_log);
            log.insert(
                last_applied_log,
                Entry::new_snapshot_pointer(
                    last_applied_log,
                    term,
                    "".into(),
                    membership_config.clone(),
                ),
            );

            let snapshot = Snapshot {
                index: last_applied_log,
                term,
                membership: membership_config.clone(),
                data,
            };
            snapshot_bytes = serde_json::to_vec(&snapshot)?;
            *current_snapshot = Some(snapshot);
        } // Release log & snapshot write locks.

        tracing::trace!(
            { snapshot_size = snapshot_bytes.len() },
            "log compaction complete"
        );
        Ok(CurrentSnapshotData {
            term,
            index: last_applied_log,
            membership: membership_config.clone(),
            snapshot: Box::new(Cursor::new(snapshot_bytes)),
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn create_snapshot(&self) -> Result<(String, Box<Self::Snapshot>)> {
        Ok((String::from(""), Box::new(Cursor::new(Vec::new())))) // Snapshot IDs are insignificant to this storage engine.
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn finalize_snapshot_installation(
        &self,
        index: u64,
        term: u64,
        delete_through: Option<u64>,
        id: String,
        snapshot: Box<Self::Snapshot>,
    ) -> Result<()> {
        tracing::trace!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );
        let raw = serde_json::to_string_pretty(snapshot.get_ref().as_slice())?;
        println!("JSON SNAP:\n{}", raw);
        let new_snapshot: Snapshot = serde_json::from_slice(snapshot.get_ref().as_slice())?;
        // Update log.
        {
            // Go backwards through the log to find the most recent membership config <= the `through` index.
            let mut log = self.log.write().await;
            let membership_config = log
                .values()
                .rev()
                .skip_while(|entry| entry.index > index)
                .find_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.id));

            match &delete_through {
                Some(through) => {
                    *log = log.split_off(&(through + 1));
                }
                None => log.clear(),
            }
            log.insert(
                index,
                Entry::new_snapshot_pointer(index, term, id, membership_config),
            );
        }

        // Update the state machine.
        {
            let (last_applied_log, entries): (u64, Vec<FullType>) =
                serde_json::from_slice(&new_snapshot.data)?;
            let mut sm = self.sm.write().await;
            let mut cache = T::default();
            for FullType {
                key,
                value,
                exp_time,
            } in entries.iter()
            {
                cache.set(key, value.to_string(), *exp_time).await;
            }
            *sm = StateMachine {
                last_applied_log,
                cache,
            };
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(&self) -> Result<Option<CurrentSnapshotData<Self::Snapshot>>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let reader = serde_json::to_vec(&snapshot)?;
                Ok(Some(CurrentSnapshotData {
                    index: snapshot.index,
                    term: snapshot.term,
                    membership: snapshot.membership.clone(),
                    snapshot: Box::new(Cursor::new(reader)),
                }))
            }
            None => Ok(None),
        }
    }
}
