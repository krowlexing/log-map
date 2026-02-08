#pragma once

#include <string>
#include <stdexcept>
#include <optional>

extern "C" {
    struct LogMapHandle;
    using logmap_handle_t = void*;

    enum ErrorCode {
        LOGMAP_SUCCESS = 0,
        LOGMAP_NULL_POINTER = 1,
        LOGMAP_INVALID_UTF8 = 2,
        LOGMAP_CONNECT_ERROR = 3,
        LOGMAP_GET_ERROR = 4,
        LOGMAP_INSERT_ERROR = 5,
        LOGMAP_REMOVE_ERROR = 6,
        LOGMAP_INTERNAL_ERROR = 99
    };

    ErrorCode logmap_connect(const char* addr, logmap_handle_t* handle_out);
    ErrorCode logmap_free(logmap_handle_t handle);
    ErrorCode logmap_get(logmap_handle_t handle, long key, char** value_out);
    ErrorCode logmap_insert(logmap_handle_t handle, long key, const char* value);
    ErrorCode logmap_remove(logmap_handle_t handle, long key);
    int logmap_contains_key(logmap_handle_t handle, long key);
    size_t logmap_len(logmap_handle_t handle);
    int logmap_is_empty(logmap_handle_t handle);
    void logmap_string_free(char* s);
}

namespace log_map {

class exception : public std::runtime_error {
public:
    explicit exception(ErrorCode code)
        : std::runtime_error(error_message(code)), _code(code) {}

    ErrorCode code() const noexcept { return _code; }

private:
    ErrorCode _code;

    static const char* error_message(ErrorCode code) {
        switch (code) {
            case LOGMAP_SUCCESS:           return "Success";
            case LOGMAP_NULL_POINTER:      return "Null pointer";
            case LOGMAP_INVALID_UTF8:      return "Invalid UTF-8";
            case LOGMAP_CONNECT_ERROR:     return "Connection error";
            case LOGMAP_GET_ERROR:         return "Get error";
            case LOGMAP_INSERT_ERROR:      return "Insert error";
            case LOGMAP_REMOVE_ERROR:      return "Remove error";
            case LOGMAP_INTERNAL_ERROR:    return "Internal error";
            default:                       return "Unknown error";
        }
    }
};

inline void check_error(ErrorCode code) {
    if (code != LOGMAP_SUCCESS) {
        throw exception(code);
    }
}

class string_result {
public:
    string_result() : _ptr(nullptr) {}
    explicit string_result(char* ptr) : _ptr(ptr) {}

    ~string_result() {
        if (_ptr) {
            logmap_string_free(_ptr);
        }
    }

    string_result(const string_result&) = delete;
    string_result& operator=(const string_result&) = delete;

    string_result(string_result&& other) noexcept : _ptr(other._ptr) {
        other._ptr = nullptr;
    }

    string_result& operator=(string_result&& other) noexcept {
        if (this != &other) {
            if (_ptr) logmap_string_free(_ptr);
            _ptr = other._ptr;
            other._ptr = nullptr;
        }
        return *this;
    }

    explicit operator bool() const noexcept { return _ptr != nullptr; }

    std::string to_string() const {
        if (!_ptr) return "";
        return _ptr;
    }

    const char* c_str() const noexcept { return _ptr; }

private:
    char* _ptr;
};

class LogMap {
public:
    LogMap() : _handle(nullptr) {}

    explicit LogMap(const std::string& addr) : _handle(nullptr) {
        connect(addr);
    }

    ~LogMap() {
        if (_handle) {
            logmap_free(_handle);
        }
    }

    LogMap(const LogMap&) = delete;
    LogMap& operator=(const LogMap&) = delete;

    LogMap(LogMap&& other) noexcept : _handle(other._handle) {
        other._handle = nullptr;
    }

    LogMap& operator=(LogMap&& other) noexcept {
        if (this != &other) {
            if (_handle) logmap_free(_handle);
            _handle = other._handle;
            other._handle = nullptr;
        }
        return *this;
    }

    void connect(const std::string& addr) {
        logmap_handle_t handle;
        check_error(logmap_connect(addr.c_str(), &handle));
        _handle = handle;
    }

    std::optional<std::string> get(long key) const {
        char* value_out;
        check_error(logmap_get(_handle, key, &value_out));

        if (!value_out) {
            return std::nullopt;
        }

        string_result result(value_out);
        return result.to_string();
    }

    void insert(long key, const std::string& value) {
        check_error(logmap_insert(_handle, key, value.c_str()));
    }

    void remove(long key) {
        check_error(logmap_remove(_handle, key));
    }

    bool contains_key(long key) const {
        return logmap_contains_key(_handle, key) != 0;
    }

    size_t len() const {
        return logmap_len(_handle);
    }

    bool is_empty() const {
        return logmap_is_empty(_handle) != 0;
    }

    logmap_handle_t native_handle() const noexcept {
        return _handle;
    }

private:
    logmap_handle_t _handle;
};

}
