#pragma once

#include <cmath>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include <algorithm>

#include "tokenize.hpp"
#include "segmentReader.hpp"

struct Hit {
    double score;
    uint32_t docId;
    std::string title;
    std::string path;
};

struct DocKey {
    uint32_t seg;
    uint32_t doc;
    bool operator==(const DocKey& o) const noexcept{ return seg == o.seg && doc == o.doc; }
};

struct DocKeyHash {
    size_t operator()(const DocKey& k) const noexcept {
        size_t h1 = std::hash<uint32_t>{}(k.seg);
        size_t h2 = std::hash<uint32_t>{}(k.doc);
        return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1<<6) + (h1>>2));
    }
};

class SegmentSearch {
public:
    explicit SegmentSearch(std::vector<SegmentReader> segs)
        : segments_(std::move(segs)) {}
    
    void loadAllMeta(){
        for(auto& s : segments_) s.loadMeta();
    }

    void refreshDeleted(){
        for(auto& s : segments_) s.loadDeleted();
    }

    std::vector<Hit> searchBM25(const std::string& query, size_t k = 10, double k1 = 1.2, double b = 0.75) const{
        std::vector<std::string> qterms;
        tokenizeLower(query, qterms);
        if(qterms.empty()) return{};

        double n = 0.0;
        uint64_t sumdl = 0;

        for(const auto& seg : segments_){
            n += seg.docCount();
            sumdl += seg.sumDocLen();
        }
        if (n <= 0.0) return {};

        double avgdl = (sumdl > 0) ? (static_cast<double>(sumdl) / n) : 1.0;
        if(avgdl <= 0.0) avgdl = 1.0;

        std::unordered_map<DocKey, double, DocKeyHash> acc;
        acc.reserve(4096);

        for(const auto& term : qterms) {
            double df = 0.0;
            for(const auto& seg : segments_){
                df += seg.df(term);
            }
            if(df <= 0.0) continue;

            double idf = std::log(1.0 + (n - df + 0.5) / (df + 0.5));

            for(size_t si = 0; si < segments_.size(); si++){
                const auto& seg = segments_[si];
                const auto& plist = seg.getPostings(term);

                for(const auto& [docId, tf_u32] : plist) {
                    if(seg.isDeleted(docId)) continue;
                    if(tf_u32 == 0) continue;
                    double dl = std::max(1.0, static_cast<double>(seg.docLen(docId)));
                    double tf = static_cast<double>(tf_u32);

                    double norm = (1.0 - b) + b * (dl / avgdl);
                    double score = idf * (tf * (k1 + 1.0)) / (tf + k1 * norm);

                    DocKey key{static_cast<uint32_t>(si), docId};
                    acc[key] += score;
                }
            }
        }

        std::vector<Hit> hits;
        hits.reserve(acc.size());

        for(const auto& kv : acc){
            if(kv.second <= 0.0) continue;

            const DocKey& key = kv.first;
            const auto& seg = segments_[key.seg];

            if(seg.isDeleted(key.doc)) continue;

            DocMeta m = seg.docMeta(key.doc);
            hits.push_back(Hit{kv.second, key.doc, m.title, m.path});
        }
        std::sort(hits.begin(), hits.end(), [](const Hit& a, const Hit& b) {
            return a.score > b.score;
        });
        if(hits.size() > k) hits.resize(k);
        return hits;
    }

private:
    std::vector<SegmentReader> segments_;
};
