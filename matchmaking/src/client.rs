//! The WebSocket matchmaking session.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddrV4, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use dolphin_integrations::Log;
use slippi_user::UserManager;
use tungstenite::client::IntoClientRequest;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

use crate::protocol::{self, Candidate, DebugOverrides};
use crate::stun::classify_nat;

const PROD_URL: &str = "wss://matchmaking.slippi.gg/v1/queue";
const DEV_URL: &str = "wss://matchmaking-dev.slippi.gg/v1/queue";

/// Environment variable that overrides the service URL, for local testing
/// against a service started with authentication disabled.
pub const URL_ENV_VAR: &str = "SLIPPI_MM_URL";

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const HANDSHAKE_READ_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_READ_TIMEOUT: Duration = Duration::from_millis(250);
const CONNECT_ATTEMPTS: u32 = 3;
/// How long after losing a queued ticket's connection we keep trying to
/// resume. The service drops a silent ticket after thirty seconds.
const RESUME_WINDOW: Duration = Duration::from_secs(15);
const RETRY_DELAY: Duration = Duration::from_secs(1);
/// How long a connection may go without the service acknowledging its join
/// or resume, matching the previous client's join timeout.
const JOIN_TIMEOUT: Duration = Duration::from_secs(5);
/// The service sends a status message every few seconds while a ticket is
/// queued. After this long without any frame we send a ping, and after
/// `LIVENESS_TIMEOUT` we treat the connection as lost. Without this a network
/// that drops silently would leave the search running forever while the
/// service had long since dropped the ticket.
const PING_AFTER: Duration = Duration::from_secs(5);
const LIVENESS_TIMEOUT: Duration = Duration::from_secs(15);

/// Where a matchmaking session is. The numeric values cross the FFI.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchmakingState {
    Idle = 0,
    Connecting = 1,
    Queued = 2,
    Matched = 3,
    Failed = 4,
}

impl MatchmakingState {
    fn from_u8(v: u8) -> Self {
        match v {
            1 => MatchmakingState::Connecting,
            2 => MatchmakingState::Queued,
            3 => MatchmakingState::Matched,
            4 => MatchmakingState::Failed,
            _ => MatchmakingState::Idle,
        }
    }
}

/// What the Dolphin side knows when it asks for a match.
#[derive(Clone, Debug, Default)]
pub struct MatchRequest {
    /// 0 ranked, 1 unranked, 2 direct, 3 teams, 4 party.
    pub mode: u8,
    /// Opponent connect code or lobby code as raw Shift-JIS bytes.
    pub connect_code: Vec<u8>,
    /// Local UDP port the ENet socket is bound to.
    pub netplay_port: u16,
    /// "ip:port" on the local network, or empty.
    pub lan_addr: String,
    /// The public address each of two STUN servers reported for the netplay
    /// socket, in the order they were asked. None where a server did not
    /// answer. Two answers are needed to classify the NAT.
    pub stun: [Option<SocketAddrV4>; 2],
    /// Optional explicit user credentials/identity instead of `user_manager`.
    pub user_override: Option<protocol::UserInfo>,
    /// Only honored by a service running with authentication disabled.
    pub debug: Option<DebugOverrides>,
}

#[derive(Debug, Default)]
struct Shared {
    state: AtomicU8,
    cancel: AtomicBool,
    result_json: Mutex<Option<String>>,
    error: Mutex<String>,
}

impl Shared {
    fn set_state(&self, s: MatchmakingState) {
        self.state.store(s as u8, Ordering::SeqCst);
    }

    fn state(&self) -> MatchmakingState {
        MatchmakingState::from_u8(self.state.load(Ordering::SeqCst))
    }

    fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }

    fn fail(&self, message: impl Into<String>) {
        let message = message.into();
        tracing::error!(target: Log::SlippiOnline, "[Matchmaking] {}", message);
        *self.error.lock().unwrap() = message;
        self.set_state(MatchmakingState::Failed);
    }
}

/// A matchmaking client. One session runs at a time; starting a new one ends
/// the previous session.
#[derive(Debug)]
pub struct MatchmakingClient {
    user_manager: UserManager,
    slippi_semver: String,
    url: String,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct SessionHandle {
    shared: Arc<Shared>,
    worker: Option<JoinHandle<()>>,
}

/// The part that changes as sessions start and end. It sits behind a mutex
/// because the Dolphin side reaches it from more than one thread: an old
/// matchmaking object winding down on a cleanup thread, or multiple local Teams
/// sessions running in parallel.
#[derive(Debug)]
struct Inner {
    next_session_id: u64,
    latest_session_id: u64,
    sessions: HashMap<u64, SessionHandle>,
}

impl MatchmakingClient {
    /// Creates a client. The service URL comes from `SLIPPI_MM_URL` when set,
    /// otherwise the dev or prod service depending on the build version.
    pub fn new(user_manager: UserManager, slippi_semver: String) -> Self {
        let url = std::env::var(URL_ENV_VAR).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| {
            if slippi_semver.contains("dev") {
                DEV_URL.to_string()
            } else {
                PROD_URL.to_string()
            }
        });

        Self::with_url(user_manager, slippi_semver, url)
    }

    fn with_url(user_manager: UserManager, slippi_semver: String, url: String) -> Self {
        Self {
            user_manager,
            slippi_semver,
            url,
            inner: Mutex::new(Inner {
                next_session_id: 0,
                latest_session_id: 0,
                sessions: HashMap::new(),
            }),
        }
    }

    /// The URL this client connects to.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Begins a session and returns its id. Any previous session is cancelled
    /// first.
    pub fn start(&self, request: MatchRequest) -> u64 {
        self.start_inner(request, true)
    }

    /// Begins a session without cancelling existing sessions, allowing multiple
    /// local Teams players on the same client to queue and match concurrently.
    pub fn start_concurrent(&self, request: MatchRequest) -> u64 {
        self.start_inner(request, false)
    }

    fn start_inner(&self, request: MatchRequest, cancel_existing: bool) -> u64 {
        let join = self.build_join(&request);
        let url = self.url.clone();
        let user_manager = self.user_manager.clone();

        let (session_id, previous) = {
            let mut inner = self.inner.lock().unwrap();
            let previous = if cancel_existing {
                inner.sessions.drain().map(|(_, h)| h).collect()
            } else {
                Vec::new()
            };

            inner.next_session_id += 1;
            let session_id = inner.next_session_id;
            inner.latest_session_id = session_id;

            let shared = Arc::new(Shared::default());
            shared.set_state(MatchmakingState::Connecting);

            tracing::info!(
                target: Log::SlippiOnline,
                "[Matchmaking] Starting session {} against {}",
                session_id,
                url
            );

            let worker_shared = shared.clone();
            let worker = thread::spawn(move || {
                Session {
                    shared: worker_shared,
                    url,
                    join,
                    user_manager,
                    ticket: None,
                    resume_deadline: None,
                    acknowledged: false,
                }
                .run();
            });

            inner.sessions.insert(
                session_id,
                SessionHandle {
                    shared,
                    worker: Some(worker),
                },
            );

            (session_id, previous)
        };
        for handle in previous {
            Self::cancel_handle(handle);
        }
        session_id
    }

    /// Cancels the session with the given id. Ids from earlier sessions are
    /// ignored, so tearing down an old caller cannot end a newer search.
    pub fn cancel_session(&self, session_id: u64) {
        let handle = self.inner.lock().unwrap().sessions.remove(&session_id);
        if let Some(handle) = handle {
            Self::cancel_handle(handle);
        }
    }

    /// Current state of the most recently started session.
    pub fn state(&self) -> MatchmakingState {
        let id = self.inner.lock().unwrap().latest_session_id;
        self.state_for(id)
    }

    /// Current state of a specific session.
    pub fn state_for(&self, session_id: u64) -> MatchmakingState {
        self.inner
            .lock()
            .unwrap()
            .sessions
            .get(&session_id)
            .map(|handle| handle.shared.state())
            .unwrap_or(MatchmakingState::Idle)
    }

    /// The match message as the service sent it, once the state is Matched.
    /// Each match is returned once.
    pub fn take_result_json(&self) -> Option<String> {
        let id = self.inner.lock().unwrap().latest_session_id;
        self.take_result_json_for(id)
    }

    /// The match message for a specific session, returned once. Removes the
    /// completed session entry once its result is taken.
    pub fn take_result_json_for(&self, session_id: u64) -> Option<String> {
        let mut inner = self.inner.lock().unwrap();
        let taken = inner
            .sessions
            .get(&session_id)
            .and_then(|handle| handle.shared.result_json.lock().unwrap().take());
        if taken.is_some() {
            if let Some(mut handle) = inner.sessions.remove(&session_id) {
                drop(handle.worker.take());
            }
        }
        taken
    }

    /// The failure reason of the latest session, once the state is Failed.
    pub fn error_message(&self) -> String {
        let id = self.inner.lock().unwrap().latest_session_id;
        self.error_message_for(id)
    }

    /// The failure reason for a specific session, once its state is Failed.
    pub fn error_message_for(&self, session_id: u64) -> String {
        self.inner
            .lock()
            .unwrap()
            .sessions
            .get(&session_id)
            .map(|handle| handle.shared.error.lock().unwrap().clone())
            .unwrap_or_default()
    }

    /// Leaves the queue and ends all sessions. Safe to call at any time and
    /// never blocks on the network.
    pub fn cancel(&self) {
        let handles: Vec<_> = self.inner.lock().unwrap().sessions.drain().map(|(_, h)| h).collect();
        for handle in handles {
            Self::cancel_handle(handle);
        }
    }

    /// Flags the running session to stop. The worker notices within one read
    /// timeout, sends `cancel` if it is connected, and exits on its own. It is
    /// not joined: a worker stuck in a connect or handshake would otherwise
    /// hold up the caller for many seconds, and it touches nothing but its
    /// own shared state once flagged.
    fn cancel_handle(mut handle: SessionHandle) {
        handle.shared.cancel.store(true, Ordering::SeqCst);
        drop(handle.worker.take());
    }

    fn build_join(&self, request: &MatchRequest) -> protocol::Join {
        let user = request.user_override.clone().unwrap_or_else(|| {
            self.user_manager.get(|user| protocol::UserInfo {
                uid: user.uid.clone(),
                play_key: user.play_key.clone(),
                connect_code: user.connect_code.clone(),
                display_name: user.display_name.clone(),
            })
        });

        // The NAT is classified from what two servers saw. The addresses themselves are
        // deliberately kept out of the log.
        let nat_type = classify_nat(request.stun[0], request.stun[1]);
        tracing::info!(target: Log::SlippiOnline, "[Matchmaking] NAT type: {}", nat_type.as_str());

        let mut candidates = Vec::new();
        let mut port = request.netplay_port;
        if let Some(reflexive) = request.stun.iter().flatten().next() {
            candidates.push(Candidate {
                ip: reflexive.ip().to_string(),
                port: reflexive.port(),
                kind: "srflx".into(),
            });
            port = reflexive.port();
        }
        if let Some((ip, lan_port)) = request.lan_addr.rsplit_once(':') {
            if let Ok(lan_port) = lan_port.parse::<u16>() {
                candidates.push(Candidate {
                    ip: ip.to_string(),
                    port: lan_port,
                    kind: "host".into(),
                });
            }
        }

        protocol::Join {
            kind: "join",
            user,
            search: protocol::Search {
                mode: request.mode,
                connect_code: request.connect_code.clone(),
            },
            app_version: self.slippi_semver.clone(),
            port,
            lan_addr: request.lan_addr.clone(),
            candidates,
            nat_type: nat_type.as_str().into(),
            debug: request.debug.clone(),
        }
    }
}

impl Drop for MatchmakingClient {
    fn drop(&mut self) {
        self.cancel();
    }
}

type Socket = WebSocket<MaybeTlsStream<TcpStream>>;

/// Why a connection ended, as seen by the session loop.
enum Ended {
    /// The session reached a terminal state; nothing more to do.
    Done,
    /// The connection dropped while the ticket may still be queued.
    Lost,
    /// The service never acknowledged the join.
    JoinTimeout,
}

struct Session {
    shared: Arc<Shared>,
    url: String,
    join: protocol::Join,
    user_manager: UserManager,
    /// Ticket ID and token once the service has accepted us.
    ticket: Option<(String, String)>,
    /// Until when a lost connection is worth resuming. Set when the connection
    /// drops with a ticket in hand and cleared when the service takes us back.
    resume_deadline: Option<Instant>,
    /// Whether the current connection's join or resume has been acknowledged.
    acknowledged: bool,
}

impl Session {
    fn run(mut self) {
        let mut connect_failures = 0;

        loop {
            if self.shared.cancelled() {
                self.shared.set_state(MatchmakingState::Idle);
                return;
            }

            let mut socket = match connect(&self.url, &self.shared) {
                Ok(socket) => socket,
                Err(ConnectError::Cancelled) => {
                    self.shared.set_state(MatchmakingState::Idle);
                    return;
                },
                Err(ConnectError::Failed(err)) => {
                    tracing::warn!(target: Log::SlippiOnline, "[Matchmaking] Connect failed: {}", err);
                    connect_failures += 1;
                    if self.give_up(connect_failures) || self.sleep_unless_cancelled(RETRY_DELAY) {
                        return;
                    }
                    continue;
                },
            };
            connect_failures = 0;

            let first = match &self.ticket {
                Some((ticket_id, token)) => serde_json::to_string(&protocol::Resume {
                    kind: "resume",
                    ticket_id: ticket_id.clone(),
                    token: token.clone(),
                }),
                None => serde_json::to_string(&self.join),
            }
            .expect("protocol message serializes");

            if let Err(err) = socket.send(Message::text(first)) {
                tracing::warn!(target: Log::SlippiOnline, "[Matchmaking] Failed to send first message: {}", err);
                let _ = socket.close(None);
                connect_failures += 1;
                if self.give_up(connect_failures) || self.sleep_unless_cancelled(RETRY_DELAY) {
                    return;
                }
                continue;
            }

            match self.serve(&mut socket) {
                Ended::Done => return,
                Ended::JoinTimeout => {
                    self.shared.fail("Failed to join mm queue");
                    return;
                },
                Ended::Lost => {
                    if self.ticket.is_none() {
                        self.shared.fail("Lost connection to the mm server");
                        return;
                    }
                    match self.resume_deadline {
                        None => self.resume_deadline = Some(Instant::now() + RESUME_WINDOW),
                        Some(deadline) if Instant::now() >= deadline => {
                            self.shared.fail("Lost connection to the mm server");
                            return;
                        },
                        Some(_) => {},
                    }
                    tracing::warn!(target: Log::SlippiOnline, "[Matchmaking] Connection lost, resuming ticket");
                    if self.sleep_unless_cancelled(RETRY_DELAY) {
                        return;
                    }
                },
            }
        }
    }

    /// Decides whether a failed connect ends the session: past the resume
    /// window when resuming, after a few attempts when first joining. Fails
    /// the session when it does.
    fn give_up(&self, connect_failures: u32) -> bool {
        let give_up = match self.resume_deadline {
            Some(deadline) => Instant::now() >= deadline,
            None => connect_failures >= CONNECT_ATTEMPTS,
        };
        if give_up {
            self.shared.fail(if self.ticket.is_some() {
                "Lost connection to the mm server"
            } else {
                "Failed to connect to mm server"
            });
        }
        give_up
    }

    /// Runs one connection until the session ends or the connection drops.
    fn serve(&mut self, socket: &mut Socket) -> Ended {
        let opened = Instant::now();
        let mut last_frame = opened;
        let mut pinged = false;
        self.acknowledged = false;

        loop {
            if self.shared.cancelled() {
                let _ = socket.send(Message::text(
                    serde_json::to_string(&protocol::Simple { kind: "cancel" }).unwrap(),
                ));
                let _ = socket.close(None);
                self.shared.set_state(MatchmakingState::Idle);
                return Ended::Done;
            }

            match socket.read() {
                Ok(Message::Text(text)) => {
                    last_frame = Instant::now();
                    pinged = false;
                    if let Some(ended) = self.handle(text.as_str(), socket) {
                        return ended;
                    }
                },
                Ok(Message::Close(frame)) => {
                    tracing::info!(target: Log::SlippiOnline, "[Matchmaking] Server closed connection: {:?}", frame);
                    return Ended::Lost;
                },
                Ok(_) => {
                    // Binary, ping, and pong frames carry nothing for us but
                    // prove the service is there. tungstenite queues pong
                    // replies; flush sends them.
                    last_frame = Instant::now();
                    pinged = false;
                    let _ = socket.flush();
                },
                Err(tungstenite::Error::Io(err)) if is_timeout(&err) => {
                    let _ = socket.flush();
                },
                Err(err) => {
                    tracing::warn!(target: Log::SlippiOnline, "[Matchmaking] Read failed: {}", err);
                    return Ended::Lost;
                },
            }

            if !self.acknowledged && opened.elapsed() >= JOIN_TIMEOUT {
                tracing::warn!(target: Log::SlippiOnline, "[Matchmaking] Service did not acknowledge the connection in time");
                let _ = socket.close(None);
                return if self.ticket.is_none() {
                    Ended::JoinTimeout
                } else {
                    Ended::Lost
                };
            }

            let idle = last_frame.elapsed();
            if idle >= LIVENESS_TIMEOUT {
                tracing::warn!(target: Log::SlippiOnline, "[Matchmaking] Nothing from the service for {:?}, connection lost", idle);
                let _ = socket.close(None);
                return Ended::Lost;
            }
            if idle >= PING_AFTER && !pinged {
                pinged = true;
                let _ = socket.send(Message::text(
                    serde_json::to_string(&protocol::Simple { kind: "ping" }).unwrap(),
                ));
            }
        }
    }

    /// Handles one server message. Returns Some when the connection is done.
    fn handle(&mut self, text: &str, socket: &mut Socket) -> Option<Ended> {
        let envelope: protocol::Envelope = match serde_json::from_str(text) {
            Ok(env) => env,
            Err(err) => {
                tracing::warn!(target: Log::SlippiOnline, "[Matchmaking] Ignoring malformed message: {}", err);
                return None;
            },
        };

        match envelope.kind.as_str() {
            "joined" => {
                if let Ok(joined) = serde_json::from_str::<protocol::Joined>(text) {
                    tracing::info!(target: Log::SlippiOnline, "[Matchmaking] Ticket {} accepted", joined.ticket_id);
                    self.ticket = Some((joined.ticket_id, joined.token));
                    self.acknowledged = true;
                    // The service took us back, so the next drop gets a full
                    // resume window of its own.
                    self.resume_deadline = None;
                    self.shared.set_state(MatchmakingState::Queued);
                }
                None
            },
            "status" => {
                if let Ok(status) = serde_json::from_str::<protocol::Status>(text) {
                    tracing::debug!(target: Log::SlippiOnline, "[Matchmaking] In queue for {:.1}s", status.seconds_in_queue);
                }
                None
            },
            "match" => {
                match serde_json::from_str::<protocol::MatchSummary>(text) {
                    Ok(summary) => tracing::info!(
                        target: Log::SlippiOnline,
                        "[Matchmaking] Match found: {} (host: {})",
                        summary.match_id,
                        summary.is_host
                    ),
                    Err(err) => tracing::warn!(target: Log::SlippiOnline, "[Matchmaking] Match message unreadable: {}", err),
                }
                *self.shared.result_json.lock().unwrap() = Some(text.to_string());
                self.shared.set_state(MatchmakingState::Matched);
                let _ = socket.close(None);
                Some(Ended::Done)
            },
            "error" => {
                let message = match serde_json::from_str::<protocol::ErrorMessage>(text) {
                    Ok(err) => {
                        if !err.latest_version.is_empty() {
                            // The service knows a newer build exists. Remember it
                            // for players whose update check is broken.
                            self.user_manager.overwrite_latest_version(err.latest_version);
                        }
                        err.error
                    },
                    Err(_) => "Received an error from the mm server".to_string(),
                };
                self.shared.fail(message);
                let _ = socket.close(None);
                Some(Ended::Done)
            },
            _ => None,
        }
    }

    /// Sleeps in small steps so a cancel is noticed. Returns true if cancelled.
    fn sleep_unless_cancelled(&self, total: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < total {
            if self.shared.cancelled() {
                self.shared.set_state(MatchmakingState::Idle);
                return true;
            }
            thread::sleep(Duration::from_millis(50));
        }
        false
    }
}

fn is_timeout(err: &std::io::Error) -> bool {
    matches!(err.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)
}

enum ConnectError {
    /// The session was cancelled while connecting.
    Cancelled,
    Failed(String),
}

/// Opens a WebSocket to `url` over IPv4 only. Netplay is IPv4, so the address
/// the service observes must be the IPv4 one. The cancel flag is checked
/// between address attempts so a cancel does not wait out every timeout.
fn connect(url: &str, shared: &Shared) -> Result<Socket, ConnectError> {
    let failed = |e: String| ConnectError::Failed(e);
    let request = url.into_client_request().map_err(|e| failed(e.to_string()))?;
    let uri = request.uri().clone();
    let host = uri.host().ok_or_else(|| failed("matchmaking URL has no host".to_string()))?;
    let secure = uri.scheme_str() == Some("wss");
    let port = uri.port_u16().unwrap_or(if secure { 443 } else { 80 });

    let addrs: Vec<_> = (host, port)
        .to_socket_addrs()
        .map_err(|e| failed(format!("resolving {}: {}", host, e)))?
        .filter(|a| a.is_ipv4())
        .collect();
    if addrs.is_empty() {
        return Err(failed(format!("no IPv4 address for {}", host)));
    }

    let mut last_err = String::new();
    for addr in addrs {
        if shared.cancelled() {
            return Err(ConnectError::Cancelled);
        }
        match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
            Ok(stream) => {
                let _ = stream.set_nodelay(true);
                let _ = stream.set_read_timeout(Some(HANDSHAKE_READ_TIMEOUT));
                let (socket, _response) = tungstenite::client_tls(request, stream).map_err(|e| failed(e.to_string()))?;
                set_read_timeout(&socket, POLL_READ_TIMEOUT);
                return Ok(socket);
            },
            Err(err) => last_err = format!("connecting to {}: {}", addr, err),
        }
    }
    Err(failed(last_err))
}

fn set_read_timeout(socket: &Socket, timeout: Duration) {
    let stream: &TcpStream = match socket.get_ref() {
        MaybeTlsStream::Plain(s) => s,
        MaybeTlsStream::Rustls(s) => s.get_ref(),
        _ => return,
    };
    let _ = stream.set_read_timeout(Some(timeout));
}

// Keep the trait imports used even when a TLS feature set changes what
// `MaybeTlsStream` exposes.
#[allow(dead_code)]
fn _assert_stream_traits<S: Read + Write>() {}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::sync::{Arc, Barrier};

    use slippi_gg_api::APIClient;

    use super::*;

    fn test_client(url: String, tag: &str) -> MatchmakingClient {
        let dir = std::env::temp_dir().join(format!("slippi-mm-unit-{}-{}", tag, std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let api = APIClient::new("3.7.0");
        let user_manager = UserManager::new(api, dir, "3.7.0".into());
        user_manager.set(|user| {
            user.uid = "default-uid".to_string();
            user.play_key = "default-key".to_string();
            user.connect_code = "DEF#1".to_string();
            user.display_name = "Default".to_string();
        });
        MatchmakingClient::with_url(user_manager, "3.7.0".into(), url)
    }

    fn wait_state(client: &MatchmakingClient, session_id: u64, target: MatchmakingState, timeout: Duration) -> MatchmakingState {
        let start = Instant::now();
        loop {
            let state = client.state_for(session_id);
            if state == target || state == MatchmakingState::Failed || start.elapsed() > timeout {
                return state;
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn four_concurrent_sessions_with_overrides() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("ws://{}/v1/queue", addr);

        let barrier = Arc::new(Barrier::new(4));
        let server = thread::spawn({
            let barrier = barrier.clone();
            move || {
                let mut workers = Vec::new();
                for _ in 0..4 {
                    let (stream, _) = listener.accept().unwrap();
                    let barrier = barrier.clone();
                    workers.push(thread::spawn(move || {
                        let mut ws = tungstenite::accept(stream).unwrap();
                        let join_msg = ws.read().unwrap().into_text().unwrap();
                        let join: serde_json::Value = serde_json::from_str(join_msg.as_str()).unwrap();
                        let port = join["port"].as_u64().unwrap();
                        let uid = join["user"]["uid"].as_str().unwrap().to_string();
                        ws.send(Message::text(
                            serde_json::json!({
                                "type": "joined",
                                "ticketId": format!("ticket-{}", port),
                                "token": format!("token-{}", port),
                            })
                            .to_string(),
                        ))
                        .unwrap();

                        // All four sessions must join concurrently before any receives a match.
                        barrier.wait();

                        ws.send(Message::text(
                            serde_json::json!({
                                "type": "match",
                                "matchId": format!("mode.teams-concurrent-{}", uid),
                                "isHost": port == 49000,
                                "port": port,
                                "uid": uid,
                            })
                            .to_string(),
                        ))
                        .unwrap();
                        let _ = ws.close(None);
                    }));
                }
                for w in workers {
                    w.join().unwrap();
                }
            }
        });

        let client = test_client(url, "four-concurrent");
        let mut sessions = Vec::new();
        for i in 0..4u16 {
            let id = client.start_concurrent(MatchRequest {
                mode: 3,
                connect_code: b"TEST".to_vec(),
                netplay_port: 49000 + i,
                lan_addr: format!("192.168.0.10:{}", 49000 + i),
                user_override: Some(protocol::UserInfo {
                    uid: format!("uid-{}", i),
                    play_key: format!("key-{}", i),
                    connect_code: format!("P{}#1", i + 1),
                    display_name: format!("Player {}", i + 1),
                }),
                ..Default::default()
            });
            sessions.push(id);
        }

        for (i, &id) in sessions.iter().enumerate() {
            assert_eq!(
                wait_state(&client, id, MatchmakingState::Matched, Duration::from_secs(5)),
                MatchmakingState::Matched,
                "session {} error: {}",
                id,
                client.error_message_for(id)
            );
            let raw = client.take_result_json_for(id).expect("expected result json");
            let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
            assert_eq!(parsed["port"], 49000 + i as u64);
            assert_eq!(parsed["uid"], format!("uid-{}", i));
            assert_eq!(parsed["matchId"], format!("mode.teams-concurrent-uid-{}", i));
            assert!(client.take_result_json_for(id).is_none());
        }

        server.join().unwrap();
    }

    #[test]
    fn cancel_one_session_keeps_other_alive() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("ws://{}/v1/queue", addr);

        let (tx_cancelled, rx_cancelled) = std::sync::mpsc::channel();
        let (tx_match2, rx_match2) = std::sync::mpsc::channel::<()>();
        let rx_match2 = Arc::new(Mutex::new(Some(rx_match2)));

        let server = thread::spawn(move || {
            let mut workers = Vec::new();
            for _ in 0..2 {
                let (stream, _) = listener.accept().unwrap();
                let _ = stream.set_read_timeout(Some(Duration::from_millis(50)));
                let tx_cancelled = tx_cancelled.clone();
                let rx_match2 = rx_match2.clone();
                workers.push(thread::spawn(move || {
                    let mut ws = tungstenite::accept(stream).unwrap();
                    let join_msg = ws.read().unwrap().into_text().unwrap();
                    let join: serde_json::Value = serde_json::from_str(join_msg.as_str()).unwrap();
                    let port = join["port"].as_u64().unwrap();
                    ws.send(Message::text(
                        serde_json::json!({
                            "type": "joined",
                            "ticketId": format!("ticket-{}", port),
                            "token": format!("token-{}", port),
                        })
                        .to_string(),
                    ))
                    .unwrap();

                    if port == 49000 {
                        let deadline = Instant::now() + Duration::from_secs(5);
                        loop {
                            assert!(Instant::now() < deadline, "timed out waiting for cancel on session 1");
                            match ws.read() {
                                Ok(Message::Text(txt)) => {
                                    let msg: serde_json::Value = serde_json::from_str(txt.as_str()).unwrap();
                                    if msg["type"] == "cancel" {
                                        tx_cancelled.send(port).unwrap();
                                        let _ = ws.close(None);
                                        return;
                                    }
                                },
                                Ok(Message::Close(_)) => {
                                    tx_cancelled.send(port).unwrap();
                                    return;
                                },
                                Err(tungstenite::Error::Io(e)) if is_timeout(&e) => continue,
                                Ok(_) => continue,
                                Err(err) => panic!("unexpected ws read error: {}", err),
                            }
                        }
                    } else {
                        let rx = rx_match2.lock().unwrap().take().unwrap();
                        rx.recv_timeout(Duration::from_secs(5)).unwrap();
                        ws.send(Message::text(
                            serde_json::json!({
                                "type": "match",
                                "matchId": "mode.teams-survivor",
                                "isHost": true,
                                "port": port,
                            })
                            .to_string(),
                        ))
                        .unwrap();
                        let _ = ws.close(None);
                    }
                }));
            }
            for w in workers {
                w.join().unwrap();
            }
        });

        let client = test_client(url, "cancel-one");
        let id1 = client.start_concurrent(MatchRequest {
            mode: 3,
            connect_code: b"TEST".to_vec(),
            netplay_port: 49000,
            ..Default::default()
        });
        let id2 = client.start_concurrent(MatchRequest {
            mode: 3,
            connect_code: b"TEST".to_vec(),
            netplay_port: 49001,
            ..Default::default()
        });

        assert_eq!(wait_state(&client, id1, MatchmakingState::Queued, Duration::from_secs(5)), MatchmakingState::Queued);
        assert_eq!(wait_state(&client, id2, MatchmakingState::Queued, Duration::from_secs(5)), MatchmakingState::Queued);

        client.cancel_session(id1);
        assert_eq!(client.state_for(id1), MatchmakingState::Idle);
        assert_eq!(rx_cancelled.recv_timeout(Duration::from_secs(5)).unwrap(), 49000);
        assert_eq!(client.state_for(id2), MatchmakingState::Queued);

        tx_match2.send(()).unwrap();
        assert_eq!(wait_state(&client, id2, MatchmakingState::Matched, Duration::from_secs(5)), MatchmakingState::Matched);
        let raw = client.take_result_json_for(id2).expect("survivor should get match json");
        let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["matchId"], "mode.teams-survivor");
        assert_eq!(parsed["port"], 49001);

        server.join().unwrap();
    }

    #[test]
    fn error_on_one_session_keeps_other_alive() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("ws://{}/v1/queue", addr);

        let barrier = Arc::new(Barrier::new(2));
        let server = thread::spawn(move || {
            let mut workers = Vec::new();
            for _ in 0..2 {
                let (stream, _) = listener.accept().unwrap();
                let barrier = barrier.clone();
                workers.push(thread::spawn(move || {
                    let mut ws = tungstenite::accept(stream).unwrap();
                    let join_msg = ws.read().unwrap().into_text().unwrap();
                    let join: serde_json::Value = serde_json::from_str(join_msg.as_str()).unwrap();
                    let port = join["port"].as_u64().unwrap();
                    ws.send(Message::text(
                        serde_json::json!({
                            "type": "joined",
                            "ticketId": format!("ticket-{}", port),
                            "token": format!("token-{}", port),
                        })
                        .to_string(),
                    ))
                    .unwrap();
                    barrier.wait();

                    if port == 49000 {
                        ws.send(Message::text(
                            serde_json::json!({
                                "type": "error",
                                "error": "boom 1",
                            })
                            .to_string(),
                        ))
                        .unwrap();
                    } else {
                        ws.send(Message::text(
                            serde_json::json!({
                                "type": "match",
                                "matchId": "mode.teams-session-2",
                                "isHost": false,
                                "port": port,
                            })
                            .to_string(),
                        ))
                        .unwrap();
                    }
                    let _ = ws.close(None);
                }));
            }
            for w in workers {
                w.join().unwrap();
            }
        });

        let client = test_client(url, "error-one");
        let id1 = client.start_concurrent(MatchRequest {
            mode: 3,
            connect_code: b"TEST".to_vec(),
            netplay_port: 49000,
            ..Default::default()
        });
        let id2 = client.start_concurrent(MatchRequest {
            mode: 3,
            connect_code: b"TEST".to_vec(),
            netplay_port: 49001,
            ..Default::default()
        });

        assert_eq!(wait_state(&client, id1, MatchmakingState::Failed, Duration::from_secs(5)), MatchmakingState::Failed);
        assert_eq!(client.error_message_for(id1), "boom 1");
        assert!(client.take_result_json_for(id1).is_none());

        assert_eq!(wait_state(&client, id2, MatchmakingState::Matched, Duration::from_secs(5)), MatchmakingState::Matched);
        let raw = client.take_result_json_for(id2).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["matchId"], "mode.teams-session-2");

        server.join().unwrap();
    }

    #[test]
    fn non_concurrent_start_cancels_existing() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("ws://{}/v1/queue", addr);

        let (tx_cancelled, rx_cancelled) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let mut workers = Vec::new();
            for idx in 0..2 {
                let (stream, _) = listener.accept().unwrap();
                let _ = stream.set_read_timeout(Some(Duration::from_millis(50)));
                let tx_cancelled = tx_cancelled.clone();
                workers.push(thread::spawn(move || {
                    let mut ws = tungstenite::accept(stream).unwrap();
                    let join_msg = ws.read().unwrap().into_text().unwrap();
                    let join: serde_json::Value = serde_json::from_str(join_msg.as_str()).unwrap();
                    let port = join["port"].as_u64().unwrap();
                    ws.send(Message::text(
                        serde_json::json!({
                            "type": "joined",
                            "ticketId": format!("ticket-{}", idx),
                            "token": format!("token-{}", idx),
                        })
                        .to_string(),
                    ))
                    .unwrap();

                    if port == 41000 {
                        let deadline = Instant::now() + Duration::from_secs(5);
                        loop {
                            assert!(Instant::now() < deadline, "session 1 was not cancelled");
                            match ws.read() {
                                Ok(Message::Text(txt)) => {
                                    let msg: serde_json::Value = serde_json::from_str(txt.as_str()).unwrap();
                                    if msg["type"] == "cancel" {
                                        tx_cancelled.send(()).unwrap();
                                        let _ = ws.close(None);
                                        return;
                                    }
                                },
                                Ok(Message::Close(_)) => {
                                    tx_cancelled.send(()).unwrap();
                                    return;
                                },
                                Err(tungstenite::Error::Io(e)) if is_timeout(&e) => continue,
                                Ok(_) => continue,
                                Err(err) => panic!("unexpected ws read error: {}", err),
                            }
                        }
                    } else {
                        ws.send(Message::text(
                            serde_json::json!({
                                "type": "match",
                                "matchId": "mode.unranked-second",
                                "isHost": true,
                            })
                            .to_string(),
                        ))
                        .unwrap();
                        let _ = ws.close(None);
                    }
                }));
            }
            for w in workers {
                w.join().unwrap();
            }
        });

        let client = test_client(url, "non-concurrent");
        let id1 = client.start(MatchRequest {
            mode: 1,
            netplay_port: 41000,
            ..Default::default()
        });
        assert_eq!(wait_state(&client, id1, MatchmakingState::Queued, Duration::from_secs(5)), MatchmakingState::Queued);

        let id2 = client.start(MatchRequest {
            mode: 1,
            netplay_port: 41001,
            ..Default::default()
        });
        assert_eq!(client.state_for(id1), MatchmakingState::Idle);
        rx_cancelled.recv_timeout(Duration::from_secs(5)).unwrap();

        assert_eq!(wait_state(&client, id2, MatchmakingState::Matched, Duration::from_secs(5)), MatchmakingState::Matched);
        let raw = client.take_result_json_for(id2).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["matchId"], "mode.unranked-second");

        server.join().unwrap();
    }
}
