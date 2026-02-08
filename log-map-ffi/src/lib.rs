use std::ffi::{c_char, c_void, CStr, CString};
use std::ptr;

type LogMapHandle = *mut c_void;

#[repr(C)]
pub enum ErrorCode {
    Success = 0,
    NullPointer = 1,
    InvalidUtf8 = 2,
    ConnectError = 3,
    GetError = 4,
    InsertError = 5,
    RemoveError = 6,
    InternalError = 99,
}

impl From<log_map::Error> for ErrorCode {
    fn from(err: log_map::Error) -> Self {
        match err {
            log_map::Error::Transport(_) => ErrorCode::ConnectError,
            log_map::Error::Status(_) => ErrorCode::GetError,
            log_map::Error::Conflict(_) => ErrorCode::InsertError,
            log_map::Error::ConnectionClosed => ErrorCode::InternalError,
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn logmap_connect(addr: *const c_char, handle_out: *mut LogMapHandle) -> ErrorCode {
    if addr.is_null() || handle_out.is_null() {
        return ErrorCode::NullPointer;
    }

    let addr_str = unsafe { CStr::from_ptr(addr) }.to_str();
    let addr = match addr_str {
        Ok(s) => s,
        Err(_) => return ErrorCode::InvalidUtf8,
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let map = rt.block_on(log_map::LogMap::connect(addr));

    let map = match map {
        Ok(m) => m,
        Err(e) => return ErrorCode::from(e),
    };

    let wrapper = LogMapWrapper { map, rt };
    let boxed = Box::new(wrapper);
    unsafe { *handle_out = Box::into_raw(boxed) as *mut c_void };

    ErrorCode::Success
}

#[unsafe(no_mangle)]
pub extern "C" fn logmap_free(handle: LogMapHandle) -> ErrorCode {
    if handle.is_null() {
        return ErrorCode::NullPointer;
    }

    unsafe {
        let _ = Box::from_raw(handle as *mut LogMapWrapper);
    }

    ErrorCode::Success
}

#[unsafe(no_mangle)]
pub extern "C" fn logmap_get(
    handle: LogMapHandle,
    key: i64,
    value_out: *mut *mut c_char,
) -> ErrorCode {
    if handle.is_null() {
        return ErrorCode::NullPointer;
    }

    let wrapper = unsafe { &*(handle as *const LogMapWrapper) };
    let rt = &wrapper.rt;

    let result = rt.block_on(wrapper.map.get(key));

    match result {
        Ok(Some(value)) => {
            let c_value = match CString::new(value) {
                Ok(s) => s.into_raw(),
                Err(_) => return ErrorCode::InvalidUtf8,
            };
            unsafe { *value_out = c_value };
            ErrorCode::Success
        }
        Ok(None) => {
            unsafe { *value_out = ptr::null_mut() };
            ErrorCode::Success
        }
        Err(e) => ErrorCode::from(e),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn logmap_insert(
    handle: LogMapHandle,
    key: i64,
    value: *const c_char,
) -> ErrorCode {
    if handle.is_null() || value.is_null() {
        return ErrorCode::NullPointer;
    }

    let value_str = unsafe { CStr::from_ptr(value) }.to_str();
    let value = match value_str {
        Ok(s) => s.to_string(),
        Err(_) => return ErrorCode::InvalidUtf8,
    };

    let wrapper = unsafe { &*(handle as *const LogMapWrapper) };
    let rt = &wrapper.rt;

    let result = rt.block_on(wrapper.map.insert(key, value));

    match result {
        Ok(_) => ErrorCode::Success,
        Err(e) => ErrorCode::from(e),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn logmap_remove(handle: LogMapHandle, key: i64) -> ErrorCode {
    if handle.is_null() {
        return ErrorCode::NullPointer;
    }

    let wrapper = unsafe { &*(handle as *const LogMapWrapper) };
    let rt = &wrapper.rt;

    let result = rt.block_on(wrapper.map.remove(key));

    match result {
        Ok(_) => ErrorCode::Success,
        Err(e) => ErrorCode::from(e),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn logmap_contains_key(handle: LogMapHandle, key: i64) -> i32 {
    if handle.is_null() {
        return 0;
    }

    let wrapper = unsafe { &*(handle as *const LogMapWrapper) };
    if wrapper.map.contains_key(key) { 1 } else { 0 }
}

#[unsafe(no_mangle)]
pub extern "C" fn logmap_len(handle: LogMapHandle) -> usize {
    if handle.is_null() {
        return 0;
    }

    let wrapper = unsafe { &*(handle as *const LogMapWrapper) };
    wrapper.map.len()
}

#[unsafe(no_mangle)]
pub extern "C" fn logmap_is_empty(handle: LogMapHandle) -> i32 {
    if handle.is_null() {
        return 1;
    }

    let wrapper = unsafe { &*(handle as *const LogMapWrapper) };
    if wrapper.map.is_empty() { 1 } else { 0 }
}

#[unsafe(no_mangle)]
pub extern "C" fn logmap_string_free(s: *mut c_char) {
    if !s.is_null() {
        unsafe { let _ = CString::from_raw(s); }
    }
}

struct LogMapWrapper {
    map: log_map::LogMap,
    rt: tokio::runtime::Runtime,
}
