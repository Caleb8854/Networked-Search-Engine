#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "doc.hpp"

struct DocMeta {
    std::string title;
    std::string path;
};

class InvertedIndex {
public:
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
};