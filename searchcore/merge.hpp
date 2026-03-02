#pragma once

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <utility>
#include <vector>
#include <stdexcept>

#include "binio.hpp"
#include "segmentReader.hpp"
#include "segmentWriter.hpp"

namespace fs = std::filesystem;

inline uint32_t readPhysicalDocCount(const fs::path& segDir) {
    std::ifstream in(segDir / "docs.bin", std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open docs.bin in " + segDir.string());
    return read_u32(in);
}

inline uint32_t readDeletedCount(const fs::path& segDir) {
    std::ifstream in(segDir / "deleted.bin", std::ios::binary);
    if (!in) return 0;
    uint32_t n = read_u32(in);
    return n;
}

struct SegmentInfo {
    fs::path dir;
    std::string name;
    uint32_t physDocs = 0;
    uint32_t delDocs  = 0;
    uint32_t liveDocs = 0;
};

inline std::vector<SegmentInfo> listSegments(const std::string& segmentsRootStr) {
    fs::path root(segmentsRootStr);
    if (!fs::exists(root) || !fs::is_directory(root)) {
        throw std::runtime_error("segments root not found: " + segmentsRootStr);
    }

    std::vector<SegmentInfo> out;

    for (auto& e : fs::directory_iterator(root)) {
        if (!e.is_directory()) continue;

        std::string name = e.path().filename().string();
        if (name.rfind("seg_", 0) != 0) continue;

        SegmentInfo info;
        info.dir = e.path();
        info.name = name;

        try {
            info.physDocs = readPhysicalDocCount(info.dir);
        } catch (...) {
            continue;
        }

        info.delDocs = 0;
        try {
            info.delDocs = readDeletedCount(info.dir);
        } catch (...) {
            info.delDocs = 0;
        }

        info.liveDocs = (info.delDocs >= info.physDocs) ? 0u : (info.physDocs - info.delDocs);

        out.push_back(std::move(info));
    }

    std::sort(out.begin(), out.end(),
        [](const SegmentInfo& a, const SegmentInfo& b) {
            if (a.liveDocs != b.liveDocs) return a.liveDocs < b.liveDocs;
            if (a.physDocs != b.physDocs) return a.physDocs < b.physDocs;
            return a.name < b.name;
        });

    return out;
}

inline void collectTerms(const SegmentReader& seg, std::unordered_set<std::string>& termsOut) {
    termsOut.reserve(termsOut.size() + seg.allTermdf().size());
    for (const auto& kv : seg.allTermdf()) {
        termsOut.insert(kv.first);
    }
}

inline uint32_t mergeSegmentsDropDeletes(const std::string& segmentsRootStr, const fs::path& segDirA, const fs::path& segDirB) {
    SegmentReader A(segDirA.string());
    SegmentReader B(segDirB.string());
    A.loadMeta();
    B.loadMeta();

    InvertedIndex merged;

    std::unordered_map<uint32_t,uint32_t> mapA;
    std::unordered_map<uint32_t,uint32_t> mapB;
    mapA.reserve(A.allDocs().size());
    mapB.reserve(B.allDocs().size());

    uint32_t nextId = 1;

    auto addLiveDocs = [&](const SegmentReader& S, std::unordered_map<uint32_t,uint32_t>& remap)
    {
        for (const auto& [oldId, dm] : S.allDocs()) {
            if (S.isDeleted(oldId)) continue;

            uint32_t newId = nextId++;
            remap.emplace(oldId, newId);

            merged.docs[newId] = dm;
            merged.doclen[newId] = S.docLen(oldId);
        }
    };

    addLiveDocs(A, mapA);
    addLiveDocs(B, mapB);

    std::unordered_set<std::string> allTerms;
    allTerms.reserve(A.allTermdf().size() + B.allTermdf().size());
    collectTerms(A, allTerms);
    collectTerms(B, allTerms);

    std::vector<std::pair<uint32_t,uint32_t>> tmp;
    for (const auto& term : allTerms) {
        tmp.clear();

        for (const auto& [oldDoc, tf] : A.getPostings(term)) {
            if (A.isDeleted(oldDoc)) continue;
            auto it = mapA.find(oldDoc);
            if (it == mapA.end()) continue;
            tmp.emplace_back(it->second, tf);
        }

        for (const auto& [oldDoc, tf] : B.getPostings(term)) {
            if (B.isDeleted(oldDoc)) continue;
            auto it = mapB.find(oldDoc);
            if (it == mapB.end()) continue;
            tmp.emplace_back(it->second, tf);
        }

        if (tmp.empty()) continue;

        std::sort(tmp.begin(), tmp.end(), [](auto& x, auto& y){ return x.first < y.first; });


        merged.postings[term] = tmp;
        merged.termdf[term] = static_cast<uint32_t>(tmp.size());
    }

    fs::path root(segmentsRootStr);
    SegmentWriter::flush(merged, root.string());

    return static_cast<uint32_t>(merged.docs.size());
}

inline uint32_t mergeSmallest(const std::string& segmentsRootStr) {
    auto segs = listSegments(segmentsRootStr);
    if (segs.size() < 2) return 0;

    fs::path a = segs[0].dir;
    fs::path b = segs[1].dir;

    uint32_t mergedLiveDocs = mergeSegmentsDropDeletes(segmentsRootStr, a, b);

    std::error_code ec;
    fs::remove_all(a, ec);
    if (ec) throw std::runtime_error("Failed to remove segment " + a.string() + ": " + ec.message());

    fs::remove_all(b, ec);
    if (ec) throw std::runtime_error("Failed to remove segment " + b.string() + ": " + ec.message());

    return mergedLiveDocs;
}