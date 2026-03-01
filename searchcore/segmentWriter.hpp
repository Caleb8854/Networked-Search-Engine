#pragma once

#include <cstdint>
#include <fstream>
#include <string>
#include <stdexcept>
#include <filesystem>
#include <chrono>
#include <unordered_map>
#include <vector>
#include <iomanip>
#include <sstream>

#include "binio.hpp"
#include "index.hpp"

namespace fs = std::filesystem;

struct SegmentMeta {
    uint32_t docCount = 0;
    uint32_t termCount = 0;
    uint64_t createdAtUnix = 0;
};

class SegmentWriter {
public:
    static SegmentMeta flush(const InvertedIndex& idx, const std::string& segmentRoot);

private:
    static void writeDocs(const InvertedIndex& idx, const fs::path& path);
    static void writeDoclen(const InvertedIndex& idx, const fs::path& path);
    static void writePostings(const InvertedIndex& idx, const fs::path& path);
    static void writeTermdf(const InvertedIndex& idx, const fs::path& path);
    static void writeMetaJson(const SegmentMeta& meta, const fs::path& path);
    static fs::path makeNewSegmentDir(const fs::path& segmentRoot);
};

inline void SegmentWriter::writeDocs(const InvertedIndex& idx, const fs::path& path){
    std::ofstream out(path, std::ios::binary);
    if(!out) throw std::runtime_error("Failed to open docs.bin");
    write_u32(out, static_cast<uint32_t>(idx.docs.size()));
    for(const auto& kv : idx.docs){
        uint32_t docId = kv.first;
        const DocMeta& m = kv.second;
        write_u32(out, docId);
        write_string(out, m.title);
        write_string(out, m.path);
    }
}

inline void SegmentWriter::writeDoclen(const InvertedIndex& idx, const fs::path& path){
    std::ofstream out(path, std::ios::binary);
    if(!out) throw std::runtime_error("Failed to open doclen.bin");

    write_u32(out, static_cast<uint32_t>(idx.doclen.size()));
    
    for(const auto& kv : idx.doclen){
        write_u32(out, kv.first);
        write_u32(out, kv.second);
    }
}

inline void SegmentWriter::writePostings(const InvertedIndex& idx, const fs::path& path){
    std::ofstream out(path, std::ios::binary);
    if (!out) throw std::runtime_error("Failed to open postings.bin for write");

    write_u32(out, static_cast<uint32_t>(idx.postings.size()));

    for (const auto& kv : idx.postings) {
        const std::string& term = kv.first;
        const auto& plist = kv.second;

        write_string(out, term);
        write_u32(out, static_cast<uint32_t>(plist.size()));

        for (const auto& [docId, tf] : plist) {
            write_u32(out, docId);
            write_u32(out, tf);
        }
    }
}

inline void SegmentWriter::writeTermdf(const InvertedIndex& idx, const fs::path& path){
    std::ofstream out(path, std::ios::binary);
    if(!out) throw std::runtime_error("Failed to open termdf.bin");

    write_u32(out, static_cast<uint32_t>(idx.termdf.size()));

    for(const auto& kv : idx.termdf){
        write_string(out, kv.first);
        write_u32(out, kv.second);
    }
}

inline void SegmentWriter::writeMetaJson(const SegmentMeta& meta, const fs::path& path){
    std::ofstream out(path, std::ios::binary);
    if(!out) throw std::runtime_error("Failed to open meta.json");

    out << "{\n";
    out << "  \"docCount\": " << meta.docCount << ",\n";
    out << "  \"termCount\": " << meta.termCount << ",\n";
    out << "  \"createdAtUnix\": " << meta.createdAtUnix << "\n";
    out << "}\n";
}

inline SegmentMeta SegmentWriter::flush(const InvertedIndex& idx, const std::string& segmentRoot) {
    fs::path root(segmentRoot);
    fs::path segDir = makeNewSegmentDir(root);

    SegmentMeta meta;
    meta.docCount = static_cast<uint32_t>(idx.docs.size());
    meta.termCount = static_cast<uint32_t>(idx.postings.size());
    meta.createdAtUnix = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count()
    );

    writeDocs(idx, segDir / "docs.bin");
    writeDoclen(idx, segDir / "doclen.bin");
    writePostings(idx, segDir / "postings.bin");
    writeTermdf(idx, segDir / "termdf.bin");
    writeMetaJson(meta, segDir / "meta.json");

    return meta;
}

inline fs::path SegmentWriter::makeNewSegmentDir(const fs::path& segmentRoot){
    fs::create_directories(segmentRoot);
    uint32_t maxId = 0;
    for(auto& e : fs::directory_iterator(segmentRoot)) {
        if(!e.is_directory()) continue;
        auto name = e.path().filename().string();
        if(name.rfind("seg_", 0) != 0) continue;
        if(name.size() != 10) continue;
        try {
            uint32_t id = static_cast<uint32_t>(std::stoul(name.substr(4)));
            if(id > maxId) maxId = id;
        } catch(...){
            continue;
        }
    }
    uint32_t nextId = maxId + 1;
    std::ostringstream ss;
    ss << "seg_" << std::setw(6) << std::setfill('0') << nextId;
    fs::path segDir = segmentRoot / ss.str();
    fs::create_directories(segDir);
    return segDir;
}