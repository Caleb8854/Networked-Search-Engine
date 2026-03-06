#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "parallelIndex.hpp"
#include "segmentWriter.hpp"
#include "segmentSearch.hpp"
#include "segmentReader.hpp"
#include "segmentOps.hpp"
#include "merge.hpp"
#include "lsmStore.hpp"

namespace py = pybind11;

static py::dict build_segment_parallel(const std::vector<std::string>& paths, uint32_t startDocId, const std::string& segmentsRoot, int threads = 0) {
    ShardedBuilder builder;
    InvertedIndex idx = builder.buildFromPaths(paths, startDocId, threads);

    SegmentMeta meta = SegmentWriter::flush(idx, segmentsRoot);

    py::dict out;
    out["docCount"] = meta.docCount;
    out["createdAtUnix"] = meta.createdAtUnix;
    out["termCount"] = meta.termCount;
    return out;
}

static std::unordered_map<std::string, uint32_t> load_indexed_paths(const std::string& segmentsRoot) {
    return SegmentReader::loadIndexedPaths(segmentsRoot);
}

static std::vector<std::tuple<double, uint32_t, std::string, std::string>> search_bm25(const std::vector<std::string>& segmentDirs, const std::string& query, int k = 10, double k1 = 1.2, double b = 0.75) {
    std::vector<std::unique_ptr<SegmentReader>> owned;
    owned.reserve(segmentDirs.size());

    std::vector<SegmentReader*> segptrs;
    segptrs.reserve(segmentDirs.size());

    for (const auto& d : segmentDirs) {
        owned.push_back(std::make_unique<SegmentReader>(d));
        segptrs.push_back(owned.back().get());
    }

    SegmentSearch s(segptrs);
    s.loadAllMeta();
    s.refreshDeleted();
    auto hits = s.searchBM25(query, k, k1, b);

    std::vector<std::tuple<double, uint32_t, std::string, std::string>> out;
    out.reserve(hits.size());
    for (auto& h : hits) {
        out.emplace_back(h.score, h.docId, h.title, h.path);
    }
    return out;
}

static uint32_t delete_by_path_all(const std::string& segmentsRoot, const std::string& path) {
    return SegmentOps::deleteByPathAll(segmentsRoot, path);
}

static uint32_t merge_smallest(const std::string& segmentsRoot) {
    return mergeSmallest(segmentsRoot);
}

PYBIND11_MODULE(searchcore, m) {
    m.doc() = "Search core bindings";

    m.def("build_segment_parallel", &build_segment_parallel,
          py::arg("paths"), py::arg("start_doc_id"), py::arg("segments_root"),
          py::arg("threads") = 0);

    m.def("load_indexed_paths", &load_indexed_paths, py::arg("segments_root"));

    m.def("search_bm25", &search_bm25,
          py::arg("segment_dirs"), py::arg("query"), py::arg("k") = 10,
          py::arg("k1") = 1.2, py::arg("b") = 0.75);

    m.def("delete_by_path_all", &delete_by_path_all,
          py::arg("segments_root"), py::arg("path"));

    m.def("merge_smallest", &merge_smallest, py::arg("segments_root"));
    
    py::class_<LsmStore>(m, "LsmStore")
        .def(py::init<const std::string&>(), py::arg("db_path"))
        .def("get_docid_by_path", &LsmStore::getDocIdByPath)
        .def("put_path", &LsmStore::putPath)
        .def("put_docmeta", &LsmStore::putDocMeta)
        .def("get_docmeta", &LsmStore::getDocMeta)
        .def("tombstone", &LsmStore::tombstone)
        .def("is_deleted", &LsmStore::isDeleted);
}