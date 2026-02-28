#pragma once

#include <string>
#include <vector>

inline void tokenizeLower(const std::string& s, std::vector<std::string>& out) {
    out.clear();

    std::string token;
    token.reserve(32);

    for (unsigned char ch : s) {
        if (ch >= 'A' && ch <= 'Z') {
            token.push_back(static_cast<char>(ch - 'A' + 'a'));
        } else if (ch >= 'a' && ch <= 'z') {
            token.push_back(static_cast<char>(ch));
        } else if (ch >= '0' && ch <= '9') {
            token.push_back(static_cast<char>(ch));
        } else {
            if (!token.empty()) {
                out.push_back(token);
                token.clear();
            }
        }
    }

    if (!token.empty()) {
        out.push_back(token);
    }
}