#pragma once

#include <mutex>
#include <sstream>
#include "../include/log_map.hpp"

namespace templet {

class logmap_wal : public write_ahead_log {
public:
    logmap_wal(const std::string& addr = "localhost:50051") : _logmap(addr), _next_index(0) {}

    void write(unsigned& index, unsigned tag, const std::string& blob) override {
        std::unique_lock<std::mutex> lock(_mut);

        std::ostringstream oss;
        oss << tag << ":" << blob.length() << ":" << blob;
        std::string value = oss.str();

        _logmap.insert(_next_index, value);
        index = _next_index++;
    }

    bool read(unsigned index, unsigned& tag, std::string& blob) override {
        std::unique_lock<std::mutex> lock(_mut);

        auto result = _logmap.get(index);
        if (!result) return false;

        const std::string& value = *result;
        std::istringstream iss(value);

        char sep;
        size_t blob_len;
        iss >> tag >> sep >> blob_len >> sep;

        blob = value.substr(iss.tellg());
        return true;
    }

private:
    log_map::LogMap _logmap;
    unsigned _next_index;
    std::mutex _mut;
};

}
