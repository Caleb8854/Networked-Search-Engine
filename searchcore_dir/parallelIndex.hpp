#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "doc.hpp"
#include "index.hpp"

class ShardedBuilder {
public:
    explicit ShardedBuilder(size_t shards = 64)
        : numShards(shards),
          shardPostings(shards),
          shardLocks(shards) {}
    
    InvertedIndex buildFromPaths(const std::vector<std::string>& paths, uint32_t startDocId, unsigned threads);

private:
    size_t numShards;
    std::vector<std::unordered_map<std::string, std::vector<std::pair<uint32_t, uint32_t>>>> shardPostings;
    std::vector<std::mutex> shardLocks;
    std::unordered_map<uint32_t, DocMeta> docs;
    std::unordered_map<uint32_t, uint32_t> doclen;
    std::mutex metaLock;
    size_t shardOf(const std::string& term) const {
        return std::hash<std::string>{}(term) % numShards;
    }
    void addDocTf(const DocTf& d);
};

inline void ShardedBuilder::addDocTf(const DocTf& d){
    {
        std::lock_guard<std::mutex> g(metaLock);
        docs[d.docId] = DocMeta{d.title,d.path};
        doclen[d.docId] = d.doclen;
    }

    for(const auto& kv : d.freqs){
        const std::string& term = kv.first;
        uint32_t tf = kv.second;
        size_t s = shardOf(term);
        std::lock_guard<std::mutex> g(shardLocks[s]);
        shardPostings[s][term].push_back({d.docId,tf});
    }
}

inline InvertedIndex ShardedBuilder::buildFromPaths(const std::vector<std::string>& paths, uint32_t startDocId, unsigned threads){
    if(threads == 0) threads = 1;
    std::atomic<uint32_t> nextId{startDocId};
    auto worker = [&](unsigned tid){
        for(size_t i = tid;i < paths.size(); i += threads){
            uint32_t docId = nextId.fetch_add(1, std::memory_order_relaxed);
            DocTf d = buildDocTf(docId, paths[i]);
            addDocTf(d);
        }
    };
    std::vector<std::thread> pool;
    pool.reserve(threads);
    for(unsigned t = 0; t < threads; t++){
        pool.emplace_back(worker,t);
    }
    for(auto& th : pool) th.join();
    
    InvertedIndex out;
    out.docs = std::move(docs);
    out.doclen = std::move(doclen);

    for(size_t s = 0; s < numShards; s++){
        for(auto& termMap : shardPostings[s]){
            const std::string& term = termMap.first;
            auto& vec = termMap.second;
            auto& dst = out.postings[term];
            dst.insert(dst.end(),vec.begin(),vec.end());
        }
    }

    out.termdf.reserve(out.postings.size());
    for(auto& kv : out.postings){
        out.termdf[kv.first] = static_cast<uint32_t>(kv.second.size());
    }
    return out;
}