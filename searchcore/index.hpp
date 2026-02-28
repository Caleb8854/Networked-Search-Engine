#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <algorithm>
#include <cmath>

#include "doc.hpp"

struct DocMeta {
    std::string title;
    std::string path;
};

class InvertedIndex {
public:
    struct SearchHit {
        double score;
        uint32_t docId;
        std::string title;
        std::string path;
    };
    std::unordered_map<std::string, std::vector<std::pair<uint32_t, uint32_t>>> postings;
    std::unordered_map<uint32_t, uint32_t> doclen;
    std::unordered_map<uint32_t, DocMeta> docs;
    std::unordered_map<std::string, uint32_t> termdf;

    void addDocument(const DocTf& d){
        docs[d.docId] = DocMeta{d.title, d.path};
        doclen[d.docId] = d.doclen;
        for(const auto& kv : d.freqs){
            const std::string& term = kv.first;
            uint32_t tf = kv.second;
            postings[term].push_back({d.docId,tf});
            termdf[term] += 1;
        }
    }

    void indexPath(uint32_t docId, const std::string& path){
        DocTf d = buildDocTf(docId, path);
        addDocument(d);
    }

    std::unordered_set<uint32_t> collectCandidates(const std::vector<std::string>& qterms) const {
        std::unordered_set<uint32_t> candidates;
        for(const auto& t : qterms){
            auto it = postings.find(t);
            if(it == postings.end())continue;
            for(const auto& [docId, tf] : it->second){
                (void)tf;
                candidates.insert(docId);
            }
        }
        return candidates;
    }

    double idf(const std::string& term) const{
        double n = static_cast<double>(docs.size());
        auto it = termdf.find(term);
        double df = (it == termdf.end()) ? 0.0 : static_cast<double>(it->second);
        return std::log((n + 1.0) / (df + 1.0)) + 1.0;
    }

    double scoreDoc(uint32_t docId, const std::vector<std::string>& qterms) const{
        auto dlit = doclen.find(docId);
        double dl = (dlit == doclen.end() || dlit->second == 0) ? 1.0 : static_cast<double>(dlit->second);
        double score = 0.0;
        for(const auto& t : qterms){
            auto pit = postings.find(t);
            if(pit == postings.end()) continue;
            uint32_t tf = 0;
            for(const auto& [d,f] : pit->second){
                if(d == docId) {
                    tf = f;
                    break;
                }
            }
            if(tf == 0) continue;
            score += static_cast<double>(tf) * idf(t);
        }
        score /= std::sqrt(dl);
        return score;
    }

    std::vector<SearchHit> search(const std::string& query, size_t k = 10)const{
        std::vector<std::string> qterms;
        tokenizeLower(query, qterms);
        if(qterms.empty()) return {};
        auto candidates = collectCandidates(qterms);
        std::vector<SearchHit> hits;
        hits.reserve(candidates.size());

        for(uint32_t docId : candidates) {
            double s = scoreDoc(docId, qterms);
            if(s <= 0.0) continue;
            auto mit = docs.find(docId);
            if(mit == docs.end()) continue;
            hits.push_back(SearchHit{s,docId,mit->second.title,mit->second.path});
        }
        std::sort(hits.begin(),hits.end(),[](const SearchHit& a,const SearchHit& b){
            return a.score > b.score;
        });
        if(hits.size() > k) hits.resize(k);
        return hits;
    }
};