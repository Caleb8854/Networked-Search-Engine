#pragma once
#include <cstdint>
#include <fstream>
#include <string>
#include <stdexcept>

inline void write_u32(std::ofstream &out, uint32_t v){
    out.write(reinterpret_cast<const char*>(&v), sizeof(v));
    if (!out) throw std::runtime_error("write_u32 failed");
}

inline uint32_t read_u32(std::ifstream &in){
    uint32_t v = 0;
    in.read(reinterpret_cast<char*>(&v), sizeof(v));
    if (!in) throw std::runtime_error("read_u32 failed");
    return v;
}

inline void write_u64(std::ofstream &out, uint64_t v){
    out.write(reinterpret_cast<const char*>(&v), sizeof(v));
    if (!out) throw std::runtime_error("write_u64 failed");
}

inline uint64_t read_u64(std::ifstream &in){
    uint64_t v = 0;
    in.read(reinterpret_cast<char*>(&v), sizeof(v));
    if (!in) throw std::runtime_error("read_u64 failed");
    return v;
}

inline void write_string(std::ofstream &out, const std::string &s){
    write_u32(out, static_cast<uint32_t>(s.size()));
    out.write(s.data(), static_cast<std::streamsize>(s.size()));
    if (!out) throw std::runtime_error("write_string failed");
}

inline std::string read_string(std::ifstream &in) {
    uint32_t n = read_u32(in);
    std::string s;
    s.resize(n);
    if (n > 0) {
        in.read(&s[0], static_cast<std::streamsize>(n));
        if (!in) throw std::runtime_error("read_string failed");
    }
    return s;
}
