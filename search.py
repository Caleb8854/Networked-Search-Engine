# from __future__ import annotations

# import json
# import os
# from dataclasses import dataclass
# from pathlib import Path
# from typing import Any, Dict, List, Tuple

import searchcore

db = searchcore.LsmStore("lsmdb")
print(db.get_docid_by_path("C:\\a.txt"))

# PROJECT_ROOT = Path(__file__).resolve().parent
# SEGMENTS_DIR = PROJECT_ROOT / "segments"
# MANIFEST_PATH = SEGMENTS_DIR / "manifest.json"
# DOCS_DIR = PROJECT_ROOT / "docs"


# @dataclass(frozen=True)
# class SearchHit:
#     score: float
#     doc_id: int
#     title: str
#     path: str


# class ManifestStore:
#     def __init__(self, segments_dir: Path, manifest_path: Path):
#         self.segments_dir = segments_dir
#         self.manifest_path = manifest_path
#         self.segments_dir.mkdir(parents=True, exist_ok=True)
#         self.manifest: Dict[str, Any] = self._load_or_init()

#     def _load_or_init(self) -> Dict[str, Any]:
#         if self.manifest_path.exists():
#             return json.loads(self.manifest_path.read_text(encoding="utf-8"))
#         return {"version": 2, "nextDocId": 1}

#     def save_atomic(self) -> None:
#         tmp = self.manifest_path.with_suffix(".json.tmp")
#         tmp.write_text(json.dumps(self.manifest, indent=2), encoding="utf-8")
#         os.replace(tmp, self.manifest_path)

#     def alloc_doc_id_block(self, n: int) -> int:
#         start = int(self.manifest.get("nextDocId", 1))
#         self.manifest["nextDocId"] = start + int(n)
#         return start

# def _list_segment_dirs(segments_root: Path) -> List[Path]:
#     if not segments_root.exists():
#         return []
#     segs = [p for p in segments_root.iterdir() if p.is_dir() and p.name.startswith("seg_")]
#     segs.sort(key=lambda p: p.name)
#     return segs


# class EngineController:
#     def __init__(self, project_root: Path):
#         self.project_root = project_root
#         self.store = ManifestStore(SEGMENTS_DIR, MANIFEST_PATH)

#     def _require(self, *names: str):
#         for n in names:
#             if hasattr(searchcore, n):
#                 return getattr(searchcore, n)
#         raise RuntimeError(
#             "searchcore is missing an expected function. "
#             f"Tried: {', '.join(names)}"
#         )

#     def list_segments(self) -> List[str]:
#         return [p.name for p in _list_segment_dirs(SEGMENTS_DIR)]

#     def _segment_paths(self) -> List[str]:
#         return [str(p) for p in _list_segment_dirs(SEGMENTS_DIR)]

#     def load_indexed_paths(self) -> Dict[str, int]:
#         fn = self._require("load_indexed_paths")
#         out = fn(str(SEGMENTS_DIR))
#         return {str(k): int(v) for k, v in out.items()}

#     def index_folder(self, docs_dir: Path, threads: int = 0) -> int:
#         if not docs_dir.is_dir():
#             raise FileNotFoundError(f"Docs folder not found: {docs_dir}")

#         indexed_live = self.load_indexed_paths()

#         paths: List[str] = []
#         for p in sorted(docs_dir.iterdir()):
#             if not p.is_file() or p.suffix.lower() != ".txt":
#                 continue
#             full = str(p.resolve())
#             if indexed_live.get(full, 0) > 0:
#                 continue
#             paths.append(full)

#         if not paths:
#             return 0

#         start_doc_id = self.store.alloc_doc_id_block(len(paths))

#         build_fn = self._require("build_segment_parallel")

#         try:
#             _meta = build_fn(paths, int(start_doc_id), str(SEGMENTS_DIR), int(threads))
#         except TypeError:
#             _meta = build_fn(paths, int(start_doc_id), str(SEGMENTS_DIR))

#         self.store.save_atomic()
#         return len(paths)

#     def delete_path(self, path: str) -> int:
#         p = Path(path)
#         if not p.is_absolute():
#             p = (self.project_root / p).resolve()
#         norm = str(p)

#         delete_fn = self._require("delete_by_path_all")
#         return int(delete_fn(str(SEGMENTS_DIR), norm))

#     def merge_smallest(self) -> int:
#         merge_fn = self._require("merge_smallest")
#         return int(merge_fn(str(SEGMENTS_DIR)))

#     def search(self, query: str, k: int = 10, k1: float = 1.2, b: float = 0.75) -> List[SearchHit]:
#         segs = self._segment_paths()
#         if not segs:
#             return []

#         search_fn = self._require("search_bm25")

#         try:
#             raw = search_fn(segs, str(query), int(k), float(k1), float(b))
#         except TypeError:
#             raw = search_fn(segs, str(query), int(k))

#         out: List[SearchHit] = []
#         for score, doc_id, title, path in raw:
#             out.append(SearchHit(float(score), int(doc_id), str(title), str(path)))
#         return out[:k]

#     def stats(self) -> None:
#         segs = _list_segment_dirs(SEGMENTS_DIR)
#         print(f"Segments: {len(segs)}")
#         print(f"Next docId: {self.store.manifest.get('nextDocId', 1)}")
#         print(f"Docs dir: {DOCS_DIR}")
#         print()

#         for i, seg in enumerate(segs, start=1):
#             meta_path = seg / "meta.json"
#             doc_count = "?"
#             created = "?"
#             if meta_path.exists():
#                 try:
#                     meta = json.loads(meta_path.read_text(encoding="utf-8"))
#                     doc_count = meta.get("docCount", "?")
#                     created = meta.get("createdAtUnix", meta.get("created_at_unix", "?"))
#                 except Exception:
#                     pass
#             print(f"  {i}. {seg.name}  docCount={doc_count}  createdAtUnix={created}")
    
#     def check_bindings(self) -> None:
#         expected = [
#             "build_segment_parallel",
#             "load_indexed_paths",
#             "search_bm25",
#             "delete_by_path_all",
#             "merge_smallest",
#         ]
#         available = [n for n in dir(searchcore) if not n.startswith("_")]
#         print("searchcore functions available:")
#         for n in available:
#             print("  ", n)
#         print()

#         missing = [n for n in expected if not hasattr(searchcore, n)]
#         if missing:
#             raise RuntimeError(f"Missing expected bindings: {missing}")
#         print("All expected bindings found")


# def main() -> None:
#     engine = EngineController(PROJECT_ROOT)

#     print("CWD:", os.getcwd())
#     print("Project root:", PROJECT_ROOT)
#     print("Segments dir:", SEGMENTS_DIR)
#     print("Docs dir:", DOCS_DIR)
#     print("\nCommands:")
#     print("  :index [threads]        -> index docs/ into a NEW segment (skips duplicates)")
#     print("  :stats                  -> show manifest + segment meta stats")
#     print("  :deletepath <path>      -> tombstone by file path across segments")
#     print("  :merge                  -> merge two smallest segments (drops deletes)")
#     print("  :check                  -> verify C++ bindings are present")
#     print("  :quit                   -> exit")
#     print("Or type a search query.\n")

#     while True:
#         q = input("search> ").strip()
#         if not q:
#             continue

#         if q == ":quit":
#             break

#         if q.startswith(":index"):
#             parts = q.split()
#             threads = int(parts[1]) if len(parts) > 1 else 0
#             n = engine.index_folder(DOCS_DIR, threads=threads)
#             print(f"Indexed {n} docs into a new segment.")
#             continue

#         if q == ":stats":
#             engine.stats()
#             continue

#         if q.startswith(":deletepath "):
#             path = q.split(" ", 1)[1].strip()
#             affected = engine.delete_path(path)
#             print(f"deleted in {affected} segment(s)" if affected > 0 else "path not found / already deleted")
#             continue

#         if q == ":merge":
#             live_docs = engine.merge_smallest()
#             print("No merge performed (need >=2 segments)." if live_docs == 0 else f"Merged; new segment liveDocs={live_docs}.")
#             continue

#         if q == ":check":
#             engine.check_bindings()
#             continue

#         results = engine.search(q, k=10)
#         if not results:
#             print("(no matches)")
#             continue

#         for hit in results:
#             print(f"  {hit.score:.3f}  {hit.doc_id}  {hit.title}  [{hit.path}]")
#         print()


# if __name__ == "__main__":
#     main()