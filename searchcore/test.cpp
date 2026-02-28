#include "doc.hpp"
#include <iostream>

int main() {
    // Make sure you have a file test_docs/a.txt with some text.
    DocTf d = buildDocTf(1, std::string(PROJECTROOT_DIR) + "/docs/b.txt");

    std::cout << "docId=" << d.docId << "\n";
    std::cout << "title=" << d.title << "\n";
    std::cout << "path=" << d.path << "\n";
    std::cout << "docLen=" << d.doclen << "\n";
    std::cout << "uniqueTerms=" << d.freqs.size() << "\n";

    // Print a few terms
    int shown = 0;
    for (auto& kv : d.freqs) {
        std::cout << kv.first << " -> " << kv.second << "\n";
        if (++shown >= 10) break;
    }
    return 0;
}