use core::panic;
use std::{
    collections::{BTreeMap, HashMap},
    hash::Hash,
    sync::{
        //mpsc::{channel, Receiver, Sender},
        Arc, Mutex,
    },
    thread::{sleep, JoinHandle},
    time::Duration,
};

use anyhow::Result;
use async_raft::{
    async_trait::async_trait,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, ClientWriteRequest, Entry, EntryPayload,
        InstallSnapshotRequest, InstallSnapshotResponse, MembershipConfig, VoteRequest,
        VoteResponse,
    },
    storage::{CurrentSnapshotData, HardState, InitialState},
    AppData, AppDataResponse, Config, NodeId, Raft, RaftNetwork, RaftStorage,
};
use log::info;
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use thiserror::Error;
use tokio::sync::RwLock;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard, mpsc::{channel, Receiver, Sender}};

use crate::{
    cache::{local::LocalCache, Cache, CachedInfo, KeyType, ValueType},
    hash::hash,
};

use super::{
    server::{GetKeyQueryParams, SetKeyJsonBody},
    Orchestrator,
};

#[derive(Debug)]
/// Possible type of requests our thread server can handle
pub enum NetworkRequest {
    GetKey(GetKeyQueryParams),
    SetKey(SetKeyJsonBody),
    /// The following variants are for Raft
    AppendEntries(AppendEntriesRequest<SetKeyJsonBody>),
    InstallSnapshot(InstallSnapshotRequest),
    Vote(VoteRequest),
}

#[derive(Debug)]
/// Responses returned by a thread
pub enum NetworkResponse {
    BaseResponse(Option<String>),
    /// The following variants are for Raft
    AppendResponse(AppendEntriesResponse),
    InstallResponse(InstallSnapshotResponse),
    VoteResponse(VoteResponse),
}

/// The response that the storage give back
#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct StorageResponse(Option<String>);

/// Simulation of a network of cache servers.
/// Each node is a thead and is mapped to an identifier.
/// The communication happens using two channels, one for the request and one for the response.
/// Sender<NetworkRequest> is used to send the network request (and is owned by Network), that is received by the thread
/// through Receiver<NetworkRequest> (owned by the thread).
/// The thread elaborate the result and send it back through Sender<NetworkResponse> (owned by the thread), and the value
/// is received by the Network struct through Receiver<NetworkResponse>
#[derive(Debug)]
pub struct Network<T>
where
    T: NetworkNode + Send + Sync + 'static,
{
    pub nodes: Mutex<HashMap<NodeId, T>>,
    pub channels: Mutex<HashMap<NodeId, (Sender<Arc<NetworkRequest>>, Receiver<Arc<NetworkResponse>>)>>,
}

impl<T> Network<T>
where
    T: NetworkNode,
{
    pub fn new() -> Network<T> {
        Network {
            nodes: Mutex::new(HashMap::new()),
            channels: Mutex::new(HashMap::new()),
        }
    }

    pub fn add_node(&mut self, id: NodeId, network: Arc<Network<T>>) {
        let (req_send, req_receive) = channel::<Arc<NetworkRequest>>(1);
        let (resp_send, resp_receive) = channel::<Arc<NetworkResponse>>(1);
        let cloned_self = Arc::clone(&network);
        let node = NetworkNode::new(id, req_receive, resp_send,cloned_self);
        self.nodes.lock().unwrap().insert(id, node);
        self.channels
            .lock()
            .unwrap()
            .insert(id, (req_send, resp_receive));
    }

    pub async fn send(&self, target: NodeId, req: Arc<NetworkRequest>) -> Arc<NetworkResponse> {
        let mut channel = self.channels.lock().unwrap();
        let sender = channel.get_mut(&target);
        match sender {
            Some(ch) => {
                let send = (*ch).0.send(Arc::clone(&req)).await.unwrap();
                let res = (*ch).1.recv().await.unwrap();
                Arc::clone(&res)
            }
            None => panic!("Not a valid target"),
        }
    }
}

pub trait NetworkNode: Send + Sync + 'static {
    fn new(
        id: NodeId,
        req_channel: Receiver<Arc<NetworkRequest>>,
        resp_channel: Sender<Arc<NetworkResponse>>,
        net: Arc<Network<Self>>,
    ) -> Self
    where
        Self: Sized;
}

impl AppData for SetKeyJsonBody {}
impl AppDataResponse for StorageResponse {}

/// Error used to trigger Raft shutdown from storage.
#[derive(Clone, Debug, Error)]
pub enum ShutdownError {
    #[error("unsafe storage error")]
    UnsafeStorageError,
}

/// The application snapshot type
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MemStoreSnapshot {
    /// The last index covered by this snapshot.
    pub index: u64,
    /// The term of the last index covered by this snapshot.
    pub term: u64,
    /// The last memberhsip config included in this snapshot.
    pub membership: MembershipConfig,
    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

/// The state machine of a node
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StateMachine<T>
where
    T: Cache,
{
    ///Last applied log
    pub last_applied_log: u64,
    /// Cache system
    pub cache: T,
}

/// Struct that represents the storage engine of a Raft node
pub struct RaftCache<T>
where
    T: Cache,
{
    /// The ID of the Raft node for which this memory storage instances is configured.
    pub id: NodeId,
    /// The Raft log.
    pub log: RwLock<BTreeMap<u64, Entry<SetKeyJsonBody>>>,
    /// The Raft state machine.
    pub sm: RwLock<StateMachine<T>>,
    /// The current hard state.
    pub hs: RwLock<Option<HardState>>,
    /// The current snapshot. We keep it in memory
    pub current_snapshot: RwLock<Option<MemStoreSnapshot>>,
}

impl<T> RaftCache<T>
where
    T: Cache,
{
    fn new(id: NodeId) -> Self {
        let state_machine = StateMachine {
            last_applied_log: 0,
            cache: T::new(),
        };
        RaftCache {
            id,
            log: RwLock::new(BTreeMap::new()),
            sm: RwLock::new(state_machine),
            hs: RwLock::new(None),
            current_snapshot: RwLock::new(None),
        }
    }
}

/// Implementation of RaftStorage trait to make RaftCache compliant with async-raft
#[async_trait]
impl<T> RaftStorage<SetKeyJsonBody, StorageResponse> for RaftCache<T>
where
    T: Cache + Sync + Send + Serialize + Deserialize<'static> + 'static,
{
    /// The storage engine's associated type used for exposing a snapshot for reading & writing.
    type Snapshot = Cursor<Vec<u8>>;

    /// The error type used to indicate to Raft that shutdown is needed when calling the
    /// `apply_entry_to_state_machine` method.
    type ShutdownError = ShutdownError;

    /// Get the latest membership config found in the log.
    ///
    /// This must always be implemented as a reverse search through the log to find the most
    /// recent membership config to be appended to the log.
    ///
    /// If a snapshot pointer is encountered, then the membership config embedded in that snapshot
    /// pointer should be used.
    ///
    /// If the system is pristine, then it should return the value of calling
    /// `MembershipConfig::new_initial(node_id)`. It is required that the storage engine persist
    /// the node's ID so that it is consistent across restarts.
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

    /// Get Raft's state information from storage.
    ///
    /// When the Raft node is first started, it will call this interface on the storage system to
    /// fetch the last known state from stable storage. If no such entry exists due to being the
    /// first time the node has come online, then `InitialState::new_initial` should be used.
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

    /// Save Raft's hard-state
    #[tracing::instrument(level = "trace", skip(self, hs))]
    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        *self.hs.write().await = Some(hs.clone());
        Ok(())
    }

    /// Get a series of log entries from storage.
    ///
    /// The start value is inclusive in the search and the stop value is non-inclusive: `[start, stop)`.
    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<SetKeyJsonBody>>> {
        // Invalid request, return empty vec.
        if start > stop {
            tracing::error!("invalid request, start > stop");
            return Ok(vec![]);
        }
        let log = self.log.read().await;
        Ok(log.range(start..stop).map(|(_, val)| val.clone()).collect())
    }

    /// Delete all logs starting from `start` and stopping at `stop`, else continuing to the end
    /// of the log if `stop` is `None`.
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

    /// Append a new entry to the log
    #[tracing::instrument(level = "trace", skip(self, entry))]
    async fn append_entry_to_log(&self, entry: &Entry<SetKeyJsonBody>) -> Result<()> {
        let mut log = self.log.write().await;
        log.insert(entry.index, entry.clone());
        Ok(())
    }

    /// Replicate a payload of entries to the log.
    ///
    /// Though the entries will always be presented in order, each entry's index should be used to
    /// determine its location to be written in the log.
    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn replicate_to_log(&self, entries: &[Entry<SetKeyJsonBody>]) -> Result<()> {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.index, entry.clone());
        }
        Ok(())
    }

    /// Apply the given log entry to the state machine.
    ///
    /// The Raft protocol guarantees that only logs which have been _committed_, that is, logs which
    /// have been replicated to a majority of the cluster, will be applied to the state machine.
    ///
    /// It is important to note that even in cases where an application specific error is returned,
    /// implementations should still record that the entry has been applied to the state machine.
    #[tracing::instrument(level = "trace", skip(self, data))]
    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        data: &SetKeyJsonBody,
    ) -> Result<StorageResponse> {
        let mut sm = self.sm.write().await;
        sm.last_applied_log = *index;
        sm.cache.set(&data.key, data.value.clone(), data.expiration);
        Ok(StorageResponse(None))
    }

    /// Apply the given payload of entries to the state machine, as part of replication.
    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn replicate_to_state_machine(&self, entries: &[(&u64, &SetKeyJsonBody)]) -> Result<()> {
        let mut sm = self.sm.write().await;
        for (index, data) in entries {
            sm.last_applied_log = **index;
            sm.cache.set(&data.key, data.value.clone(), data.expiration);
        }
        Ok(())
    }

    /// Perform log compaction, returning a handle to the generated snapshot.
    ///
    /// ### implementation guide
    /// When performing log compaction, the compaction can only cover the breadth of the log up to
    /// the last applied log and under write load this value may change quickly. As such, the
    /// storage implementation should export/checkpoint/snapshot its state machine, and then use
    /// the value of that export's last applied log as the metadata indicating the breadth of the
    /// log covered by the snapshot.
    #[tracing::instrument(level = "trace", skip(self))]
    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>> {
        let (data, last_applied_log);
        {
            // Serialize the data of the state machine.
            let sm = self.sm.read().await;
            data = serde_json::to_vec(&*sm)?;
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
                .ok_or_else(|| anyhow::anyhow!("Error"))?;
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

            let snapshot = MemStoreSnapshot {
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

    /// Create a new blank snapshot, returning a writable handle to the snapshot object along with
    /// the ID of the snapshot.
    #[tracing::instrument(level = "trace", skip(self))]
    async fn create_snapshot(&self) -> Result<(String, Box<Self::Snapshot>)> {
        Ok((String::from(""), Box::new(Cursor::new(Vec::new())))) // Snapshot IDs are insignificant to this storage engine.
    }

    /// Finalize the installation of a snapshot which has finished streaming from the cluster leader.
    ///
    /// Delete all entries in the log through `delete_through`, unless `None`, in which case
    /// all entries of the log are to be deleted.
    ///
    /// Write a new snapshot pointer to the log at the given `index`. The snapshot pointer should be
    /// constructed via the `Entry::new_snapshot_pointer` constructor and the other parameters
    /// provided to this method.
    ///
    /// All other snapshots should be deleted at this point
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

        let new_snapshot: &mut MemStoreSnapshot =
            Box::leak(serde_json::from_slice(snapshot.get_ref().as_slice())?);
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
            let new_sm: StateMachine<T> = serde_json::from_slice(&new_snapshot.data)?;
            let mut sm = self.sm.write().await;
            *sm = new_sm;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot.clone());
        Ok(())
    }

    /// Get a readable handle to the current snapshot, along with its metadata
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

/// Implementation of RaftNetwork trait to show how Raft should communicate with the other nodes
#[async_trait]
impl<T> RaftNetwork<SetKeyJsonBody> for Network<T>
where
    T: NetworkNode,
{
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<SetKeyJsonBody>,
    ) -> Result<AppendEntriesResponse> {
        
        let res = self.send(target, Arc::new(NetworkRequest::AppendEntries(rpc))).await;
    
        match *res {
            NetworkResponse::BaseResponse(_) => panic!("Expected AppendResponse"),
            NetworkResponse::AppendResponse(r) => Ok(r),
            NetworkResponse::InstallResponse(_) => panic!("Expected AppendResponse"),
            NetworkResponse::VoteResponse(_) => panic!("Expected AppendResponse"),
        }
        
    }

    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let res = self.send(target, NetworkRequest::InstallSnapshot(rpc));
        match res {
            NetworkResponse::BaseResponse(_) => panic!("Expected InstallResponse"),
            NetworkResponse::AppendResponse(_) => panic!("Expected InstallResponse"),
            NetworkResponse::InstallResponse(r) => Ok(r),
            NetworkResponse::VoteResponse(_) => panic!("Expected InstallResponse"),
        }
    }

    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        let res = self.send(target, NetworkRequest::Vote(rpc));
        match res {
            NetworkResponse::BaseResponse(_) => panic!("Expected VoteResponse"),
            NetworkResponse::AppendResponse(_) => panic!("Expected VoteResponse"),
            NetworkResponse::InstallResponse(_) => panic!("Expected VoteResponse"),
            NetworkResponse::VoteResponse(r) => Ok(r),
        }
    }
}

/// Raft node
type MemraftedNode<N, C> = Raft<SetKeyJsonBody, StorageResponse, Network<N>, RaftCache<C>>;

pub struct RaftNode {
    id: NodeId,
    //thread: JoinHandle<()>
}

impl NetworkNode for RaftNode {
    fn new(
        id: NodeId,
        req_channel: Receiver<Arc<NetworkRequest>>,
        resp_channel: Sender<Arc<NetworkResponse>>,
        net: Arc<Network<Self>>,
    ) -> Self {
        let thread = tokio::spawn(async move {
            let config = Arc::new(
                Config::build("primary-raft-group".into())
                    .validate()
                    .expect("failed to build Raft config"),
            );
            let storage = Arc::new(RaftCache::<LocalCache>::new(id));
            let node = MemraftedNode::new(id, config, net, storage);
            
            loop {
                let request = req_channel.recv().await.unwrap();
                match *request {
                    NetworkRequest::GetKey(query_params) => {
                        let mut sm = storage.sm.blocking_write();
                        let res = sm.cache.get(&query_params.key);
                        info!("Getting key {} from node {}", query_params.key, id.clone());
                        //let resp_unlocked = resp_channel.lock().unwrap();
                        resp_channel
                            .send(Arc::new(NetworkResponse::BaseResponse(res)))
                            .await
                            .unwrap();
                    }
                    NetworkRequest::SetKey(json_body) => {
                        node.client_write(ClientWriteRequest::new(json_body));
                        info!("Setting key {} to node {}", json_body.key, id.clone());
                        //let resp_unlocked = resp_channel.lock().unwrap();
                        resp_channel
                            .send(Arc::new(NetworkResponse::BaseResponse(None)))
                            .await
                            .unwrap();
                    }
                    NetworkRequest::AppendEntries(rpc) => {
                        let res = node.append_entries(rpc).await.unwrap();
                        //let resp_unlocked = resp_channel.lock().unwrap();
                        resp_channel
                            .send(Arc::new(NetworkResponse::AppendResponse(res)))
                            .await
                            .unwrap();
                    }
                    NetworkRequest::InstallSnapshot(rpc) => {
                        let res = node.install_snapshot(rpc).await.unwrap();
                        //let resp_unlocked = resp_channel.lock().unwrap();
                        resp_channel
                            .send(Arc::new(NetworkResponse::InstallResponse(res)))
                            .await
                            .unwrap();
                    }
                    NetworkRequest::Vote(rpc) => {
                        let res = node.vote(rpc).await.unwrap();
                        //let resp_unlocked = resp_channel.lock().unwrap();
                        resp_channel
                            .send(Arc::new(NetworkResponse::VoteResponse(res)))
                            .await
                            .unwrap();
                    }
                }
            };

            //panic!("Panicked from node {}", id)
        });
        RaftNode { id }
    }
}

pub struct RaftOrchestrator<N, C>
where
    N: NetworkNode,
    C: Cache + Sync + Send + Serialize + Deserialize<'static> + 'static,
{
    pub last_node: NodeId,
    pub raft_network: HashMap<NodeId, MemraftedNode<N, C>>,
    /// Map of server key with the actual server
    // cache_map: HashMap<String, T>,
    pub nodes: HashMap<String, NodeId>,
    /// Map between the hashed server key and the server key itself
    pub ring: BTreeMap<u64, NodeId>,
}

impl<N, C> RaftOrchestrator<N, C>
where
    N: NetworkNode,
    C: Cache + Sync + Send + Serialize + Deserialize<'static> + 'static,
{
    /// Implementation of "Ketama Consistent Hashing".
    /// The server key is hashed, and inserted in the ring, possibly multiple times.
    /// When we want to cache a key, we hash the key, and find in the ring the first
    /// server with the hashed key greater then the hash of the cached key.
    /// If there isn't one, get the first from the start
    fn get_target_node(&self, hashed_key: u64) -> Option<NodeId> {
        if self.ring.keys().len() == 0 {
            return None;
        }

        // Find the server associated with the first hash greater then the hashed key
        let first_greater_hash = self.ring.keys().find(|elem| **elem > hashed_key);

        let cache_idx_opt = match first_greater_hash {
            Some(idx) => Some(self.ring.get(idx).unwrap()),
            None => {
                // If there isn't a hash greater then our key, then get the first
                // from the start
                let idx = self.ring.keys().min().unwrap();
                Some(self.ring.get(idx).unwrap())
            }
        };

        if let Some(idx) = cache_idx_opt {
            return Some(*idx);
        }
        None
    }
}

#[async_trait]
impl<N, C> Cache for RaftOrchestrator<N, C>
where
    N: NetworkNode,
    C: Cache + Sync + Send + Serialize + Deserialize<'static> + 'static,
{
    fn new() -> Self {
        RaftOrchestrator {
            last_node: 0,
            raft_network: HashMap::new(),
            nodes: HashMap::new(),
            ring: BTreeMap::new(),
        }
    }

    fn get(&mut self, key: &KeyType) -> Option<ValueType> {
        let hashed_key = hash(key);
        let target = self.get_target_node(hashed_key);
        match target {
            Some(t) => {
                let node = self.raft_network.get(&t).unwrap();
                let leader = node.current_leader().await.unwrap();
                let req = GetKeyQueryParams { key: key.clone() };
                let res = self.raft_network.send(leader, NetworkRequest::GetKey(req));
                if let NetworkResponse::BaseResponse(s) = res {
                    return s;
                }
                None
            }
            None => None,
        }
    }

    fn set(&mut self, key: &KeyType, value: ValueType, expiration: u64) {
        let hashed_key = hash(key);
        let target = self.get_target_node(hashed_key);
        if let Some(t) = target {
            let req = SetKeyJsonBody {
                key: key.clone(),
                value,
                expiration,
            };
            let node = self.raft_network.get(&t).unwrap();
            let rpc = ClientWriteRequest::new(SetKeyJsonBody {
                key: String::from(key),
                value,
                expiration,
            });
            let res = (*node).client_write(rpc).await;
            //self.network.send(t, NetworkRequest::SetKey(req));
        }
    }
}

impl<N, C> Orchestrator for RaftOrchestrator<N, C>
where
    N: NetworkNode,
    C: Cache + Sync + Send + Serialize + Deserialize<'static> + 'static,
{
    ///Add a new cache to the pool
    fn add_cache(&mut self, name: String) {
        let node_id = self.last_node + 1;
        self.network.add_node(node_id);
        self.nodes.insert(name.clone(), node_id);

        let mut keys = vec![];
        for i in 0..100 {
            keys.push(format!("{}_{}", name, i));
        }
        // Compute the hash of the server name
        let cache_hash = hash(&name);

        info!(
            "Adding server {} with hashed <T> where T: Cachekey {}",
            name, cache_hash
        );

        for key in keys {
            self.ring.insert(hash(&key), node_id);
        }
    }
}
