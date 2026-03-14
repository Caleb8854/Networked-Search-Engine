#pragma once

#include <cstdint>
#include <fstream>
#include <string>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <utility>
#include <filesystem>

#include "binio.hpp"
#include "index.hpp"
#include "segmentOps.hpp"

namespace fs = std::filesystem;

class SegmentReader {
public:
    explicit SegmentReader(const std::string& segDir)
        : dir(segDir),
          postingsBin(dir / "postings.bin"),
          postingsIdx(dir / "postings.idx") {}

    void loadMeta();
    void loadDeleted();
    static std::unordered_map<std::string, uint32_t> loadIndexedPaths(const fs::path&);

    const std::vector<std::pair<uint32_t,uint32_t>>& getPostings(const std::string& term) const;

    uint32_t docCount() const { return static_cast<uint32_t>(docs.size()); }
    uint64_t sumDocLen() const { return sumDoclen; }
    uint32_t df(const std::string& term) const {
        auto it = termdf.find(term);
        return (it == termdf.end()) ? 0u : it->second;
    }
    uint32_t docLen(uint32_t docId) const {
        auto it = doclen.find(docId);
        return (it == doclen.end()) ? 0u : it->second;
    }
    DocMeta docMeta(uint32_t docId) const {
        auto it = docs.find(docId);
        return (it == docs.end()) ? DocMeta{} : it->second;
    }

    bool isDeleted(uint32_t docId) const {
        return deleted.count(docId) != 0;
    }

    const std::unordered_map<uint32_t, DocMeta>& allDocs() const { return docs; }
    const std::unordered_map<uint32_t, uint32_t>& allDoclen() const { return doclen; }
    const std::unordered_map<std::string, uint32_t>& allTermdf() const { return termdf; }

private:
    fs::path dir;
    fs::path postingsBin;
    fs::path postingsIdx;

    uint64_t sumDoclen = 0;

    std::unordered_map<std::string, uint64_t> termToOffset;
    std::unordered_map<uint32_t, DocMeta> docs;
    std::unordered_map<uint32_t, uint32_t> doclen;
    std::unordered_map<std::string, uint32_t> termdf;

    mutable std::unordered_map<std::string, std::vector<std::pair<uint32_t,uint32_t>>> cache;

    mutable std::ifstream postingsStream;

    std::unordered_set<uint32_t> deleted;

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

    for (uint32_t i = 0; i < n; i++) {
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

    for (uint32_t i = 0; i < n; i++) {
        uint32_t docId = read_u32(in);
        uint32_t dl = read_u32(in);
        doclen[docId] = dl;
    }
}

inline void SegmentReader::readTermdf() {
    std::ifstream in(dir / "termdf.bin", std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open termdf.bin");

    uint32_t n = read_u32(in);
    termdf.reserve(n);

    for (uint32_t i = 0; i < n; i++) {
        std::string term = read_string(in);
        uint32_t df = read_u32(in);
        termdf[std::move(term)] = df;
    }
}

inline void SegmentReader::readPostingsIndex() {
    std::ifstream in(postingsIdx, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open postings.idx");

    uint32_t n = read_u32(in);
    termToOffset.reserve(n);

    for (uint32_t i = 0; i < n; i++) {
        std::string term = read_string(in);
        uint64_t off = read_u64(in);
        termToOffset.emplace(std::move(term), off);
    }
}

inline void SegmentReader::loadMeta() {
    docs.clear();
    doclen.clear();
    termdf.clear();
    termToOffset.clear();
    cache.clear();
    postingsStream.close();
    postingsStream.clear();

    readDocs();
    readDoclen();

    loadDeleted();
    
    sumDoclen = 0;
    for(const auto& [docId, dl] : doclen) {
        if(!isDeleted(docId)) sumDoclen += dl;
    }

    readTermdf();

    postingsStream.open(postingsBin, std::ios::binary);
    if(!postingsStream) throw std::runtime_error("Failed to open postings.bin");

    readPostingsIndex();
}

inline const std::vector<std::pair<uint32_t,uint32_t>>& SegmentReader::getPostings(const std::string& term) const {
    auto cit = cache.find(term);
    if(cit != cache.end()) return cit->second;

    auto it = termToOffset.find(term);
    if(it == termToOffset.end()) {
        static const std::vector<std::pair<uint32_t,uint32_t>> empty;
        return empty;
    }

    postingsStream.clear();
    postingsStream.seekg(static_cast<std::streamoff>(it->second), std::ios::beg);
    if(!postingsStream) throw std::runtime_error("seekg failed in postings.bin");

    std::string termOnDisk = read_string(postingsStream);
    if(termOnDisk != term){
        throw std::runtime_error("term mismatch");
    }

    uint32_t pcount = read_u32(postingsStream);

    std::vector<std::pair<uint32_t,uint32_t>> plist;
    plist.reserve(pcount);
    for(uint32_t j = 0; j < pcount; j++){
        uint32_t docId = read_u32(postingsStream);
        uint32_t tf = read_u32(postingsStream);
        plist.emplace_back(docId, tf);
    }

    auto [insIt, ok] = cache.emplace(term, std::move(plist));
    return insIt->second;
}

inline void SegmentReader::loadDeleted(){
    deleted = SegmentOps::readDeletedSet(std::filesystem::path(dir));
}

inline std::unordered_map<std::string, uint32_t> SegmentReader::loadIndexedPaths(const fs::path& segRoot){
    std::unordered_map<std::string, uint32_t> seen;

    if (!fs::exists(segRoot) || !fs::is_directory(segRoot)) {
        return seen;
    }

    for (const auto& e : fs::directory_iterator(segRoot)) {
        if (!e.is_directory()) continue;

        const std::string name = e.path().filename().string();
        if (name.rfind("seg_", 0) != 0) continue;

        try {
            SegmentReader r(e.path().string());
            r.loadMeta();
            r.loadDeleted();

            for (const auto& kv : r.allDocs()) {
                const uint32_t docId = kv.first;
                const DocMeta& dm = kv.second;

                if(r.isDeleted(docId)) continue;

                seen[SegmentOps::normPath(dm.path)]++;
            }
        } catch (...) {
            continue;
        }
    }

    return seen;
}