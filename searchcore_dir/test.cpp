#include <iostream>
#include <filesystem>
#include <vector>
#include <stdexcept>

#include "segmentReader.hpp"
#include "segmentOps.hpp"      // if mergeSmallest is here
// or include the correct header where mergeSmallest lives
#include "merge.hpp"

namespace fs = std::filesystem;

int main() {
    try {
        // Build segments root path:
        fs::path segmentsRoot = fs::path(std::string(PROJECTROOT_DIR)) / "segments";

        if (!fs::exists(segmentsRoot) || !fs::is_directory(segmentsRoot)) {
            throw std::runtime_error("segments folder not found: " + segmentsRoot.string());
        }

        // Count segments BEFORE
        std::vector<fs::path> before;
        for (auto& e : fs::directory_iterator(segmentsRoot)) {
            if (!e.is_directory()) continue;
            if (e.path().filename().string().rfind("seg_", 0) == 0)
                before.push_back(e.path());
        }

        std::cout << "Segments before merge: " << before.size() << "\n";

        if (before.size() < 2) {
            std::cout << "Not enough segments to merge.\n";
            return 0;
        }

        // Perform merge
        uint32_t mergedLiveDocs = mergeSmallest(segmentsRoot.string());

        if (mergedLiveDocs == 0) {
            throw std::runtime_error("mergeSmallest returned 0 unexpectedly");
        }

        std::cout << "Merged live docs: " << mergedLiveDocs << "\n";

        // Count segments AFTER
        std::vector<fs::path> after;
        for (auto& e : fs::directory_iterator(segmentsRoot)) {
            if (!e.is_directory()) continue;
            if (e.path().filename().string().rfind("seg_", 0) == 0)
                after.push_back(e.path());
        }

        std::cout << "Segments after merge: " << after.size() << "\n";

        if (after.size() != before.size() - 1) {
            throw std::runtime_error("Segment count did not decrease by 1 after merge");
        }

        // Find newest segment (assume merge creates newest)
        fs::path newest;
        std::filesystem::file_time_type newestTime{};

        for (auto& seg : after) {
            auto t = fs::last_write_time(seg / "docs.bin");
            if (newest.empty() || t > newestTime) {
                newest = seg;
                newestTime = t;
            }
        }

        if (newest.empty()) {
            throw std::runtime_error("Failed to locate merged segment");
        }

        std::cout << "Merged segment directory: " << newest << "\n";

        // Load merged segment
        SegmentReader reader(newest.string());
        reader.loadMeta();

        // Verify doc count matches return value
        uint32_t actualDocs = static_cast<uint32_t>(reader.allDocs().size());
        if (actualDocs != mergedLiveDocs) {
            throw std::runtime_error(
                "Doc count mismatch. Expected " +
                std::to_string(mergedLiveDocs) +
                " got " +
                std::to_string(actualDocs)
            );
        }

        // Verify no deleted docs remain
        for (const auto& [docId, dm] : reader.allDocs()) {
            if (reader.isDeleted(docId)) {
                throw std::runtime_error("Merged segment contains deleted docs");
            }
        }

        std::cout << "Merge test passed successfully.\n";
        return 0;
    }
    catch (const std::exception& ex) {
        std::cerr << "Merge test FAILED: " << ex.what() << "\n";
        return 1;
    }
}