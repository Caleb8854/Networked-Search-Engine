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
        : dir(segDir) {}

    InvertedIndex loadAll() const;

private:
    fs::path dir;

    void readDocs(InvertedIndex& out) const;
    void readDoclen(InvertedIndex& out) const;
    void readTermdf(InvertedIndex& out) const;
    void readPostings(InvertedIndex& out) const;
};

inline void SegmentReader::readDocs(InvertedIndex& out) const {
    std::ifstream in(dir / "docs.bin", std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open docs.bin");

    uint32_t n = read_u32(in);
    out.docs.reserve(n);

    for (uint32_t i = 0; i < n; ++i) {
        uint32_t docId = read_u32(in);
        std::string title = read_string(in);
        std::string path  = read_string(in);
        out.docs[docId] = DocMeta{std::move(title), std::move(path)};
    }
}

inline void SegmentReader::readDoclen(InvertedIndex& out) const {
    std::ifstream in(dir / "doclen.bin", std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open doclen.bin");

    uint32_t n = read_u32(in);
    out.doclen.reserve(n);

    for (uint32_t i = 0; i < n; ++i) {
        uint32_t docId = read_u32(in);
        uint32_t dl    = read_u32(in);
        out.doclen[docId] = dl;
    }
}

inline void SegmentReader::readTermdf(InvertedIndex& out) const {
    std::ifstream in(dir / "termdf.bin", std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open termdf.bin");

    uint32_t n = read_u32(in);
    out.termdf.reserve(n);

    for (uint32_t i = 0; i < n; ++i) {
        std::string term = read_string(in);
        uint32_t df      = read_u32(in);
        out.termdf[std::move(term)] = df;
    }
}

inline void SegmentReader::readPostings(InvertedIndex& out) const {
    std::ifstream in(dir / "postings.bin", std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open postings.bin");

    uint32_t termCount = read_u32(in);
    out.postings.reserve(termCount);

    for (uint32_t i = 0; i < termCount; ++i) {
        std::string term = read_string(in);
        uint32_t pcount  = read_u32(in);

        auto& vec = out.postings[std::move(term)];
        vec.reserve(pcount);

        for (uint32_t j = 0; j < pcount; ++j) {
            uint32_t docId = read_u32(in);
            uint32_t tf    = read_u32(in);
            vec.emplace_back(docId, tf);
        }

        auto it = out.termdf.find(vec.empty() ? "" : vec.size() ? "" : "");
        (void)it;
    }
}

inline InvertedIndex SegmentReader::loadAll() const {
    InvertedIndex out;
    readDocs(out);
    readDoclen(out);
    readTermdf(out);
    readPostings(out);
    return out;
}