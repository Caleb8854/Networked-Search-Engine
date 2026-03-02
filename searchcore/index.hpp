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
    std::unordered_map<std::string, std::vector<std::pair<uint32_t, uint32_t>>> postings;
    std::unordered_map<uint32_t, uint32_t> doclen;
    std::unordered_map<uint32_t, DocMeta> docs;
    std::unordered_map<std::string, uint32_t> termdf;
};