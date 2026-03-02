#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <unordered_set>
#include <stdexcept>
#include <vector>
#include <algorithm>

#include "binio.hpp"

namespace fs = std::filesystem;

namespace SegmentOps {

inline std::string normPath(const std::string& p) {
    return fs::path(p).lexically_normal().string();
}

inline fs::path deletedPath(const fs::path& segDir) {
    return segDir / "deleted.bin";
}

inline std::unordered_set<uint32_t> readDeletedSet(const fs::path& segDir) {
    std::unordered_set<uint32_t> del;
    std::ifstream in(deletedPath(segDir), std::ios::binary);
    if (!in) return del;

    uint32_t n = read_u32(in);
    del.reserve(n + 1);
    for (uint32_t i = 0; i < n; i++) {
        del.insert(read_u32(in));
    }
    return del;
}

inline void writeDeletedSet(const fs::path& segDir, const std::unordered_set<uint32_t>& del) {
    fs::path tmp = deletedPath(segDir);
    tmp += ".tmp";

    std::ofstream out(tmp, std::ios::binary);
    if (!out) throw std::runtime_error("Failed to open deleted.bin.tmp for write");

    write_u32(out, static_cast<uint32_t>(del.size()));

    std::vector<uint32_t> ids;
    ids.reserve(del.size());
    for (auto id : del) ids.push_back(id);
    std::sort(ids.begin(), ids.end());

    for (auto id : ids) write_u32(out, id);

    out.close();
    fs::rename(tmp, deletedPath(segDir));
}

inline bool markDeletedDocId(const std::string& segDirStr, uint32_t docId) {
    fs::path segDir(segDirStr);

    auto del = readDeletedSet(segDir);
    size_t before = del.size();
    del.insert(docId);
    if (del.size() == before) return false;

    writeDeletedSet(segDir, del);
    return true;
}

inline bool findDocIdByPathInSegment(const std::string& segDirStr, const std::string& targetPathNorm, uint32_t& outDocId) {
    fs::path segDir(segDirStr);
    std::ifstream in(segDir / "docs.bin", std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open docs.bin for read: " + segDirStr);

    uint32_t n = read_u32(in);
    for (uint32_t i = 0; i < n; i++) {
        uint32_t docId = read_u32(in);
        (void)read_string(in);
        std::string path = read_string(in);
        if (normPath(path) == targetPathNorm) {
            outDocId = docId;
            return true;
        }
    }
    return false;
}

inline uint32_t deleteByPathAll(const std::string& segmentsRootStr, const std::string& pathToDelete) {
    fs::path root(segmentsRootStr);
    if (!fs::exists(root) || !fs::is_directory(root)) {
        throw std::runtime_error("segments root not found: " + segmentsRootStr);
    }

    std::string targetNorm = normPath(pathToDelete);

    uint32_t deletedCount = 0;

    for (auto& e : fs::directory_iterator(root)) {
        if (!e.is_directory()) continue;

        std::string name = e.path().filename().string();
        if (name.rfind("seg_", 0) != 0) continue;

        uint32_t docId = 0;
        if (findDocIdByPathInSegment(e.path().string(), targetNorm, docId)) {
            if (markDeletedDocId(e.path().string(), docId)) {
                deletedCount += 1;
            }
        }
    }

    return deletedCount;
}

}