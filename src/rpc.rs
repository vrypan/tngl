use crate::group::{self, GroupState, MemberEntry};
use crate::protocol::{
    RequestMessage, ResponseMessage, assert_eof, close_send, read_frame, write_frame,
};
use crate::state::{Entry, FolderState, TreeNode, hex};
use iroh::endpoint::{Connection, ConnectionError};
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::{Endpoint, PublicKey};
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock, mpsc};

pub const ALPN: &[u8] = b"lil/rpc/1";

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

fn next_request_id() -> u64 {
    NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Clone)]
pub struct FolderRpc {
    state: Arc<RwLock<FolderState>>,
    group: Arc<RwLock<GroupState>>,
    invites_path: Arc<PathBuf>,
    events: mpsc::UnboundedSender<RpcEvent>,
}

#[derive(Debug, Clone)]
pub enum RpcEvent {
    PeerJoined {
        peer: PublicKey,
        name: Option<String>,
    },
}

impl FolderRpc {
    pub fn new(
        state: Arc<RwLock<FolderState>>,
        group: Arc<RwLock<GroupState>>,
        invites_path: PathBuf,
        events: mpsc::UnboundedSender<RpcEvent>,
    ) -> Self {
        Self {
            state,
            group,
            invites_path: Arc::new(invites_path),
            events,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RpcClient {
    endpoint: Endpoint,
    connections: Arc<Mutex<HashMap<PublicKey, Connection>>>,
}

impl RpcClient {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get_root(&self, peer: PublicKey) -> io::Result<([u8; 32], [u8; 32], u64)> {
        let request_id = next_request_id();
        match self
            .round_trip(peer, RequestMessage::GetRoot { request_id })
            .await?
        {
            ResponseMessage::Root {
                request_id: actual,
                state_root,
                live_root,
                lamport,
            } if actual == request_id => Ok((state_root, live_root, lamport)),
            ResponseMessage::Error {
                request_id: actual,
                message,
            } if actual == request_id => Err(io::Error::other(message)),
            response => Err(io::Error::other(format!(
                "unexpected GetRoot response from {peer}: {response:?}"
            ))),
        }
    }

    pub async fn get_node(&self, peer: PublicKey, prefix: &str) -> io::Result<Option<TreeNode>> {
        let request_id = next_request_id();
        let request = RequestMessage::GetNode {
            request_id,
            prefix: prefix.to_string(),
        };
        match self.round_trip(peer, request).await? {
            ResponseMessage::Node {
                request_id: actual,
                node,
            } if actual == request_id => Ok(node),
            ResponseMessage::Error {
                request_id: actual,
                message,
            } if actual == request_id => Err(io::Error::other(message)),
            response => Err(io::Error::other(format!(
                "unexpected GetNode response from {peer}: {response:?}"
            ))),
        }
    }

    pub async fn get_entry(&self, peer: PublicKey, path: &str) -> io::Result<Option<Entry>> {
        let request_id = next_request_id();
        let request = RequestMessage::GetEntry {
            request_id,
            path: path.to_string(),
        };
        match self.round_trip(peer, request).await? {
            ResponseMessage::Entry {
                request_id: actual,
                entry,
            } if actual == request_id => Ok(entry),
            ResponseMessage::Error {
                request_id: actual,
                message,
            } if actual == request_id => Err(io::Error::other(message)),
            response => Err(io::Error::other(format!(
                "unexpected GetEntry response from {peer}: {response:?}"
            ))),
        }
    }

    pub async fn get_object_to_file(
        &self,
        peer: PublicKey,
        content_hash: [u8; 32],
        expected_size: u64,
        dest: &Path,
    ) -> io::Result<()> {
        let request_id = next_request_id();
        let request = RequestMessage::GetObject {
            request_id,
            content_hash,
        };
        let connection = self.connection(peer).await?;
        match get_object_to_file_on_connection(
            &connection,
            peer,
            request_id,
            &request,
            content_hash,
            expected_size,
            dest,
        )
        .await
        {
            Ok(()) => Ok(()),
            Err(first_err) => {
                self.drop_connection(peer).await;
                tracing::debug!(target: "lil::rpc", %peer, "retrying GetObject after: {first_err}");
                let connection = self.connection(peer).await?;
                get_object_to_file_on_connection(
                    &connection,
                    peer,
                    request_id,
                    &request,
                    content_hash,
                    expected_size,
                    dest,
                )
                .await
            }
        }
    }

    pub async fn join_group(
        &self,
        peer: PublicKey,
        secret: String,
        joiner_id: PublicKey,
        name: Option<String>,
    ) -> io::Result<(String, Vec<MemberEntry>)> {
        let request_id = next_request_id();
        let request = RequestMessage::Join {
            request_id,
            secret,
            joiner_id: joiner_id.to_string(),
            name,
        };
        match self.round_trip(peer, request).await? {
            ResponseMessage::JoinAccepted {
                request_id: actual,
                topic_id,
                members,
            } if actual == request_id => Ok((topic_id, members)),
            ResponseMessage::JoinRejected {
                request_id: actual,
                reason,
            } if actual == request_id => Err(io::Error::other(reason)),
            ResponseMessage::Error {
                request_id: actual,
                message,
            } if actual == request_id => Err(io::Error::other(message)),
            response => Err(io::Error::other(format!(
                "unexpected Join response from {peer}: {response:?}"
            ))),
        }
    }

    async fn round_trip(
        &self,
        peer: PublicKey,
        request: RequestMessage,
    ) -> io::Result<ResponseMessage> {
        let connection = self.connection(peer).await?;
        match round_trip_on_connection(&connection, peer, &request).await {
            Ok(response) => Ok(response),
            Err(first_err) => {
                self.drop_connection(peer).await;
                tracing::debug!(target: "lil::rpc", %peer, "retrying RPC after connection error: {first_err}");
                let connection = self.connection(peer).await?;
                round_trip_on_connection(&connection, peer, &request).await
            }
        }
    }

    async fn connection(&self, peer: PublicKey) -> io::Result<Connection> {
        let mut connections = self.connections.lock().await;
        if let Some(connection) = connections.get(&peer) {
            if connection.close_reason().is_none() {
                return Ok(connection.clone());
            }
            connections.remove(&peer);
        }

        let connection = self
            .endpoint
            .connect(peer, ALPN)
            .await
            .map_err(|err| io::Error::other(format!("connect to {peer}: {err}")))?;
        connections.insert(peer, connection.clone());
        Ok(connection)
    }

    async fn drop_connection(&self, peer: PublicKey) {
        self.connections.lock().await.remove(&peer);
    }
}

impl ProtocolHandler for FolderRpc {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        handle_connection(
            connection,
            Arc::clone(&self.state),
            Arc::clone(&self.group),
            Arc::clone(&self.invites_path),
            self.events.clone(),
        )
        .await
        .map_err(AcceptError::from_err)
    }
}

async fn handle_connection(
    connection: Connection,
    state: Arc<RwLock<FolderState>>,
    group: Arc<RwLock<GroupState>>,
    invites_path: Arc<PathBuf>,
    events: mpsc::UnboundedSender<RpcEvent>,
) -> io::Result<()> {
    let peer = connection.remote_id();
    tracing::debug!(target: "lil::rpc", %peer, "incoming connection");

    loop {
        let (mut send, mut recv) = match connection.accept_bi().await {
            Ok(streams) => streams,
            Err(err) if is_routine_connection_close(&err) => {
                tracing::debug!(target: "lil::rpc", %peer, "connection closed: {err}");
                return Ok(());
            }
            Err(err) => return Err(io::Error::other(format!("accept bi stream: {err}"))),
        };

        let request: RequestMessage = read_frame(&mut recv)
            .await
            .map_err(|err| io::Error::other(format!("read request from {peer}: {err}")))?;
        tracing::debug!(target: "lil::rpc", %peer, "recv {:?}", request);
        assert_eof(&mut recv)
            .await
            .map_err(|err| io::Error::other(format!("trailing bytes from {peer}: {err}")))?;

        if let RequestMessage::GetObject {
            request_id,
            content_hash,
        } = request
        {
            handle_get_object(&mut send, request_id, content_hash, peer, &state, &group).await?;
        } else {
            let response =
                handle_request(request, peer, &state, &group, &invites_path, &events).await;
            tracing::debug!(target: "lil::rpc", %peer, "send {:?}", response);
            write_frame(&mut send, &response)
                .await
                .map_err(|err| io::Error::other(format!("write response to {peer}: {err}")))?;
        }
        close_send(&mut send)
            .await
            .map_err(|err| io::Error::other(format!("close response stream to {peer}: {err}")))?;
    }
}

async fn handle_get_object(
    send: &mut iroh::endpoint::SendStream,
    request_id: u64,
    content_hash: [u8; 32],
    peer: PublicKey,
    state: &Arc<RwLock<FolderState>>,
    group: &Arc<RwLock<GroupState>>,
) -> io::Result<()> {
    if !group.read().await.is_active_member(&peer) {
        return write_frame(
            send,
            &ResponseMessage::Error {
                request_id,
                message: "peer is not a group member".to_string(),
            },
        )
        .await;
    }

    let path = state.read().await.object_path(content_hash);

    match path {
        None => {
            write_frame(
                send,
                &ResponseMessage::Error {
                    request_id,
                    message: format!("object {} not found", hex(content_hash)),
                },
            )
            .await
        }
        Some(path) => {
            let mut file = tokio::fs::File::open(&path)
                .await
                .map_err(|err| io::Error::other(format!("open {}: {err}", path.display())))?;
            let size = file.metadata().await?.len();
            write_frame(send, &ResponseMessage::ObjectHeader { request_id, size }).await?;
            tokio::io::copy(&mut file, send).await?;
            Ok(())
        }
    }
}

async fn check_member(
    group: &Arc<RwLock<GroupState>>,
    peer: PublicKey,
    request_id: u64,
) -> Result<(), ResponseMessage> {
    if group.read().await.is_active_member(&peer) {
        Ok(())
    } else {
        Err(ResponseMessage::Error {
            request_id,
            message: "peer is not a group member".to_string(),
        })
    }
}

async fn handle_request(
    request: RequestMessage,
    peer: PublicKey,
    state: &Arc<RwLock<FolderState>>,
    group: &Arc<RwLock<GroupState>>,
    invites_path: &Path,
    events: &mpsc::UnboundedSender<RpcEvent>,
) -> ResponseMessage {
    match request {
        RequestMessage::Join {
            request_id,
            secret,
            joiner_id,
            name,
        } => {
            if joiner_id != peer.to_string() {
                return ResponseMessage::JoinRejected {
                    request_id,
                    reason: "joiner id does not match RPC peer".to_string(),
                };
            }
            match group::consume_invite(invites_path, &secret) {
                Ok(true) => {
                    let mut group = group.write().await;
                    match group.add_active_peer(peer, name.clone()) {
                        Ok(_) => {
                            let _ = events.send(RpcEvent::PeerJoined { peer, name });
                            ResponseMessage::JoinAccepted {
                                request_id,
                                topic_id: hex(*group.topic_id().as_bytes()),
                                members: group.members(),
                            }
                        }
                        Err(err) => ResponseMessage::JoinRejected {
                            request_id,
                            reason: format!("could not add peer: {err}"),
                        },
                    }
                }
                Ok(false) => ResponseMessage::JoinRejected {
                    request_id,
                    reason: "invalid or expired ticket".to_string(),
                },
                Err(err) => ResponseMessage::JoinRejected {
                    request_id,
                    reason: format!("could not validate ticket: {err}"),
                },
            }
        }
        RequestMessage::GetRoot { request_id } => {
            if let Err(e) = check_member(group, peer, request_id).await {
                return e;
            }
            let state = state.read().await;
            ResponseMessage::Root {
                request_id,
                state_root: state.root_hash(),
                live_root: state.live_root_hash(),
                lamport: state.lamport(),
            }
        }
        RequestMessage::GetNode { request_id, prefix } => {
            if let Err(e) = check_member(group, peer, request_id).await {
                return e;
            }
            let state = state.read().await;
            ResponseMessage::Node {
                request_id,
                node: state.node(&prefix),
            }
        }
        RequestMessage::GetEntry { request_id, path } => {
            if let Err(e) = check_member(group, peer, request_id).await {
                return e;
            }
            let state = state.read().await;
            ResponseMessage::Entry {
                request_id,
                entry: state.entry(&path),
            }
        }
        RequestMessage::GetObject { .. } => unreachable!("handled before handle_request"),
    }
}

async fn round_trip_on_connection(
    connection: &Connection,
    peer: PublicKey,
    request: &RequestMessage,
) -> io::Result<ResponseMessage> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| io::Error::other(format!("open stream to {peer}: {err}")))?;

    write_frame(&mut send, &request)
        .await
        .map_err(|err| io::Error::other(format!("write request to {peer}: {err}")))?;
    close_send(&mut send)
        .await
        .map_err(|err| io::Error::other(format!("close request stream to {peer}: {err}")))?;

    let response = read_frame(&mut recv)
        .await
        .map_err(|err| io::Error::other(format!("read response from {peer}: {err}")))?;
    assert_eof(&mut recv)
        .await
        .map_err(|err| io::Error::other(format!("trailing response from {peer}: {err}")))?;
    Ok(response)
}

async fn get_object_to_file_on_connection(
    connection: &Connection,
    peer: PublicKey,
    request_id: u64,
    request: &RequestMessage,
    expected_hash: [u8; 32],
    expected_size: u64,
    dest: &Path,
) -> io::Result<()> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| io::Error::other(format!("open stream to {peer}: {err}")))?;

    write_frame(&mut send, request)
        .await
        .map_err(|err| io::Error::other(format!("write request to {peer}: {err}")))?;
    close_send(&mut send)
        .await
        .map_err(|err| io::Error::other(format!("close request stream to {peer}: {err}")))?;

    let header: ResponseMessage = read_frame(&mut recv)
        .await
        .map_err(|err| io::Error::other(format!("read object header from {peer}: {err}")))?;

    let size = match header {
        ResponseMessage::ObjectHeader {
            request_id: actual,
            size,
        } if actual == request_id => size,
        ResponseMessage::Error {
            request_id: actual,
            message,
        } if actual == request_id => return Err(io::Error::other(message)),
        response => {
            return Err(io::Error::other(format!(
                "unexpected GetObject response from {peer}: {response:?}"
            )));
        }
    };

    if size != expected_size {
        return Err(io::Error::other(format!(
            "object size mismatch from {peer}: expected {expected_size}, got {size}"
        )));
    }

    let result = stream_to_file(&mut recv, size, expected_hash, dest).await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(dest).await;
    }
    result?;

    assert_eof(&mut recv)
        .await
        .map_err(|err| io::Error::other(format!("trailing bytes from {peer}: {err}")))?;
    Ok(())
}

async fn stream_to_file(
    recv: &mut iroh::endpoint::RecvStream,
    size: u64,
    expected_hash: [u8; 32],
    dest: &Path,
) -> io::Result<()> {
    let mut file = tokio::fs::File::create(dest).await?;
    let mut hasher = blake3::Hasher::new();
    let mut remaining = size;
    let mut buf = vec![0u8; 256 * 1024];

    while remaining > 0 {
        let to_read = (remaining as usize).min(buf.len());
        let n = recv
            .read(&mut buf[..to_read])
            .await
            .map_err(io::Error::other)?
            .ok_or_else(|| io::Error::other("unexpected EOF during object transfer"))?;
        hasher.update(&buf[..n]);
        file.write_all(&buf[..n]).await?;
        remaining -= n as u64;
    }

    file.sync_all().await?;

    let actual_hash = *hasher.finalize().as_bytes();
    if actual_hash != expected_hash {
        return Err(io::Error::other(format!(
            "object hash mismatch: expected {}, got {}",
            hex(expected_hash),
            hex(actual_hash),
        )));
    }

    Ok(())
}

fn is_routine_connection_close(err: &ConnectionError) -> bool {
    matches!(
        err,
        ConnectionError::ApplicationClosed(_) | ConnectionError::LocallyClosed
    )
}
