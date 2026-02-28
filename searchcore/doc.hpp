#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "tokenize.hpp"

namespace fs = std::filesystem;

inline std::string readFileAll(const std::string& path){
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("Could not open file: " + path);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

inline std::string titleFromPath(const std::string& path){
    try {
        return fs::path(path).stem().string();
    } catch(...) {
        return path;
    }
}

struct DocTf {
    uint32_t docId = 0;
    std::string title;
    std::string path;
    uint32_t doclen = 0;
    std::unordered_map<std::string, uint32_t> freqs;
};

inline DocTf buildDocTf(uint32_t docId, const std::string& path){
    DocTf out;
    out.docId = docId;
    out.path = path;
    out.title = titleFromPath(path);
    
    std::string text = readFileAll(path);
    std::vector<std::string> tokens;
    tokens.reserve(1024);
    tokenizeLower(out.title + " " + text, tokens);

    out.doclen = static_cast<uint32_t>(tokens.size());

    out.freqs.reserve(tokens.size());
    for(const auto& t : tokens){
        out.freqs[t] += 1;
    }
    
    return out;
}