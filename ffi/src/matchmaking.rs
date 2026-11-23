use std::ffi::{CString, c_char, c_int};
use std::net::{Ipv4Addr, SocketAddrV4};

use slippi_exi_device::SlippiEXIDevice;
use slippi_matchmaking::{MatchRequest, REQUEST_LEN, UserInfo, build_binding_request, parse_binding_response};

use crate::c_str_to_string;

/// What one STUN server reported for the netplay socket. Filled in by
/// `slprs_stun_parse_response`; leave it zeroed for a server that did not
/// answer.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SlippiStunObservation {
    /// False when the server did not answer, in which case the other fields
    /// are ignored.
    pub answered: bool,
    /// Public IPv4 address as four octets.
    pub ip: [u8; 4],
    /// Public port in host order.
    pub port: u16,
}

impl SlippiStunObservation {
    fn to_addr(self) -> Option<SocketAddrV4> {
        self.answered.then(|| SocketAddrV4::new(Ipv4Addr::from(self.ip), self.port))
    }
}

/// Everything the Dolphin side knows when it asks for a match. Strings must be
/// valid, possibly empty, C strings; they are copied during the call.
#[repr(C)]
pub struct SlippiMatchmakingRequest {
    /// 0 ranked, 1 unranked, 2 direct, 3 teams, 4 party.
    pub mode: u8,
    /// Opponent connect code or lobby code as the raw Shift-JIS bytes the
    /// game provides. May be null when the length is zero.
    pub connect_code: *const u8,
    pub connect_code_len: usize,
    /// Local UDP port the ENet host is bound to.
    pub netplay_port: u16,
    /// "ip:port" on the local network, or empty.
    pub lan_addr: *const c_char,
    /// What each of two STUN servers reported for the netplay socket, in the
    /// order they were asked. The NAT is classified from them on this side.
    pub stun: [SlippiStunObservation; 2],
    /// When true, do not cancel other active matchmaking sessions on the same
    /// device (used by local Teams players sharing one Dolphin process).
    pub allow_concurrent: bool,
    /// Optional explicit user identity overriding `UserManager`; ignored unless
    /// both `user_uid` and `user_play_key` are non-null and non-empty.
    pub user_uid: *const c_char,
    pub user_play_key: *const c_char,
    pub user_connect_code: *const c_char,
    pub user_display_name: *const c_char,
}

fn opt_c_str_to_string(ptr: *const c_char, fn_label: &str, err_label: &str) -> String {
    if ptr.is_null() {
        String::new()
    } else {
        c_str_to_string(ptr, fn_label, err_label)
    }
}

/// Starts a matchmaking session and returns its id. Unless
/// `request.allow_concurrent` is set, any session already running is cancelled
/// first. Progress is polled with `slprs_mm_state`.
#[unsafe(no_mangle)]
pub extern "C" fn slprs_mm_start(exi_device_instance_ptr: usize, request: SlippiMatchmakingRequest) -> u64 {
    if exi_device_instance_ptr == 0 {
        return 0;
    }
    let fn_name = "slprs_mm_start";

    let connect_code = if request.connect_code.is_null() || request.connect_code_len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(request.connect_code, request.connect_code_len) }.to_vec()
    };
    let lan_addr = opt_c_str_to_string(request.lan_addr, fn_name, "lan_addr");
    let stun = [request.stun[0].to_addr(), request.stun[1].to_addr()];

    let uid = opt_c_str_to_string(request.user_uid, fn_name, "user_uid");
    let play_key = opt_c_str_to_string(request.user_play_key, fn_name, "user_play_key");
    let user_connect_code = opt_c_str_to_string(request.user_connect_code, fn_name, "user_connect_code");
    let display_name = opt_c_str_to_string(request.user_display_name, fn_name, "user_display_name");
    let user_override = if !uid.is_empty() && !play_key.is_empty() {
        Some(UserInfo {
            uid,
            play_key,
            connect_code: user_connect_code,
            display_name,
        })
    } else {
        None
    };

    let req = MatchRequest {
        mode: request.mode,
        connect_code,
        netplay_port: request.netplay_port,
        lan_addr,
        stun,
        user_override,
        debug: None,
    };

    // Use a shared reference so concurrent local-Teams matchmaking threads can
    // start/poll/cancel sessions simultaneously without `&mut` aliasing UB.
    let device = unsafe { &*(exi_device_instance_ptr as *const SlippiEXIDevice) };
    if request.allow_concurrent {
        device.matchmaking.start_concurrent(req)
    } else {
        device.matchmaking.start(req)
    }
}

/// Current session state: 0 idle, 1 connecting, 2 queued, 3 matched, 4 failed.
#[unsafe(no_mangle)]
pub extern "C" fn slprs_mm_state(exi_device_instance_ptr: usize, session_id: u64) -> c_int {
    if exi_device_instance_ptr == 0 {
        return 0;
    }
    let device = unsafe { &*(exi_device_instance_ptr as *const SlippiEXIDevice) };
    device.matchmaking.state_for(session_id) as c_int
}

/// Once the state is matched, returns the match message exactly as the service
/// sent it, or null if it was already taken. Free with `slprs_mm_free_string`.
#[unsafe(no_mangle)]
pub extern "C" fn slprs_mm_take_result_json(exi_device_instance_ptr: usize, session_id: u64) -> *mut c_char {
    if exi_device_instance_ptr == 0 {
        return std::ptr::null_mut();
    }
    let device = unsafe { &*(exi_device_instance_ptr as *const SlippiEXIDevice) };
    match device.matchmaking.take_result_json_for(session_id) {
        Some(json) => CString::new(json).map(CString::into_raw).unwrap_or(std::ptr::null_mut()),
        None => std::ptr::null_mut(),
    }
}

/// Once the state is failed, returns why. Free with `slprs_mm_free_string`.
#[unsafe(no_mangle)]
pub extern "C" fn slprs_mm_error_message(exi_device_instance_ptr: usize, session_id: u64) -> *mut c_char {
    if exi_device_instance_ptr == 0 {
        return std::ptr::null_mut();
    }
    let device = unsafe { &*(exi_device_instance_ptr as *const SlippiEXIDevice) };
    CString::new(device.matchmaking.error_message_for(session_id))
        .map(CString::into_raw)
        .unwrap_or(std::ptr::null_mut())
}

/// Leaves the queue and ends the session with the given id. A stale id from an
/// earlier session is ignored so an old matchmaking object being torn down
/// cannot cancel a newer search.
#[unsafe(no_mangle)]
pub extern "C" fn slprs_mm_cancel(exi_device_instance_ptr: usize, session_id: u64) {
    if exi_device_instance_ptr == 0 {
        return;
    }
    let device = unsafe { &*(exi_device_instance_ptr as *const SlippiEXIDevice) };
    device.matchmaking.cancel_session(session_id);
}

/// Frees a string returned by another `slprs_mm_*` function.
#[unsafe(no_mangle)]
pub extern "C" fn slprs_mm_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(unsafe { CString::from_raw(ptr) });
    }
}

/// Size in bytes of the buffer `slprs_stun_build_request` fills.
pub const SLPRS_STUN_REQUEST_LEN: usize = 20;
const _: () = assert!(SLPRS_STUN_REQUEST_LEN == REQUEST_LEN, "STUN request size drifted");

/// Writes a STUN binding request with a fresh transaction id into `out`, which
/// must have room for `SLPRS_STUN_REQUEST_LEN` bytes. Send it to a STUN server
/// from the netplay socket, then pass the reply to `slprs_stun_parse_response`.
#[unsafe(no_mangle)]
pub extern "C" fn slprs_stun_build_request(out: *mut u8) {
    let out = unsafe { &mut *(out as *mut [u8; REQUEST_LEN]) };
    build_binding_request(out);
}

/// Reads the public IPv4 address and port out of a STUN reply to `request`
/// into `out`. Returns false, leaving `out` untouched, if the bytes are not a
/// success reply to that request.
#[unsafe(no_mangle)]
pub extern "C" fn slprs_stun_parse_response(
    request: *const u8,
    response: *const u8,
    response_len: usize,
    out: *mut SlippiStunObservation,
) -> bool {
    if request.is_null() || response.is_null() || out.is_null() {
        return false;
    }
    let request = unsafe { std::slice::from_raw_parts(request, REQUEST_LEN) };
    let response = unsafe { std::slice::from_raw_parts(response, response_len) };

    match parse_binding_response(request, response) {
        Some(addr) => {
            unsafe {
                *out = SlippiStunObservation {
                    answered: true,
                    ip: addr.ip().octets(),
                    port: addr.port(),
                };
            }
            true
        },
        None => false,
    }
}
