#pragma once

#include <cstdint>
#include <fstream>
#include <string>
#include <stdexcept>
#include <unordered_map>
#include <vector>
#include <utility>
#include <filesystem>

#include "binio.hpp"
#include "index.hpp"

namespace fs = std::filesystem;

class SegmentReader {
public:
    explicit SegmentReader(const std::string& segDir)
        : dir(segDir),
          postingsBin(dir / "postings.bin"),
          postingsIdx(dir / "postings.idx") {}

    void loadMeta();

    const std::vector<std::pair<uint32_t,uint32_t>>& getPostings(const std::string& term);
    
    std::unordered_map<uint32_t, DocMeta> docs;
    std::unordered_map<uint32_t, uint32_t> doclen;
    std::unordered_map<std::string, uint32_t> termdf;


private:
    fs::path dir;
    fs::path postingsBin;
    fs::path postingsIdx;

    std::unordered_map<std::string, uint64_t> termToOffset;

    std::unordered_map<std::string, std::vector<std::pair<uint32_t,uint32_t>>> cache;

    void readDocs();
    void readDoclen();
    void readTermdf();
    void readPostingsIndex();
};

inline void SegmentReader::readDocs() {
    std::ifstream in(dir / "docs.bin", std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open docs.bin");

    uint32_t n = read_u32(in);
    docs.reserve(n);

    for (uint32_t i = 0; i < n; ++i) {
        uint32_t docId = read_u32(in);
        std::string title = read_string(in);
        std::string path  = read_string(in);
        docs[docId] = DocMeta{std::move(title), std::move(path)};
    }
}

inline void SegmentReader::readDoclen() {
    std::ifstream in(dir / "doclen.bin", std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open doclen.bin");

    uint32_t n = read_u32(in);
    doclen.reserve(n);

    for (uint32_t i = 0; i < n; ++i) {
        uint32_t docId = read_u32(in);
        uint32_t dl    = read_u32(in);
        doclen[docId] = dl;
    }
}

inline void SegmentReader::readTermdf() {
    std::ifstream in(dir / "termdf.bin", std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open termdf.bin");

    uint32_t n = read_u32(in);
    termdf.reserve(n);

    for (uint32_t i = 0; i < n; ++i) {
        std::string term = read_string(in);
        uint32_t df      = read_u32(in);
        termdf[std::move(term)] = df;
    }
}

inline void SegmentReader::readPostingsIndex() {
    std::ifstream in(postingsIdx, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open postings.idx");

    uint32_t n = read_u32(in);
    termToOffset.reserve(n);

    for (uint32_t i = 0; i < termCount; ++i) {
        std::string term = read_string(in);
        uint64_t off  = read_u64(in);
        termToOffset.emplace(std::move(term), off);
    }
}

inline InvertedIndex SegmentReader::loadMeta() {
    readDocs();
    readDoclen();
    readTermdf();
    readPostingsIndex();
    return out;
}