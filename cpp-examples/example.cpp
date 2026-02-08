#include <iostream>
#include "../include/log_map.hpp"

int main() {
    try {
        log_map::LogMap map("localhost:50051");

        map.insert(1, "hello");
        map.insert(2, "world");

        auto value = map.get(1);
        if (value) {
            std::cout << "Key 1: " << *value << std::endl;
        }

        auto value2 = map.get(2);
        if (value2) {
            std::cout << "Key 2: " << *value2 << std::endl;
        }

        auto missing = map.get(999);
        if (!missing) {
            std::cout << "Key 999 not found" << std::endl;
        }

        std::cout << "Size: " << map.len() << std::endl;
        std::cout << "Contains key 1: " << map.contains_key(1) << std::endl;

        map.remove(1);
        std::cout << "After remove, contains key 1: " << map.contains_key(1) << std::endl;

    } catch (const log_map::exception& e) {
        std::cerr << "Error: " << e.what() << " (code: " << e.code() << ")" << std::endl;
        return 1;
    }

    return 0;
}
