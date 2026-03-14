from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import searchcore

PROJECT_ROOT = Path(__file__).resolve().parent
SEGMENTS_DIR = PROJECT_ROOT / "segments"
DOCS_DIR = PROJECT_ROOT / "docs"


@dataclass(frozen=True)
class SearchHit:
    score: float
    doc_id: int
    title: str
    path: str

def _list_segment_dirs(segments_root: Path) -> List[Path]:
    if not segments_root.exists():
        return []
    segs = [p for p in segments_root.iterdir() if p.is_dir() and p.name.startswith("seg_")]
    segs.sort(key=lambda p: p.name)
    return segs


class EngineController:
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.db = searchcore.LsmStore(str(self.project_root / "lsmdb"))

    def _require(self, *names: str):
        for n in names:
            if hasattr(searchcore, n):
                return getattr(searchcore, n)
        raise RuntimeError(
            "searchcore is missing an expected function. "
            f"Tried: {', '.join(names)}"
        )

    def list_segments(self) -> List[str]:
        return [p.name for p in _list_segment_dirs(SEGMENTS_DIR)]

    def _segment_paths(self) -> List[str]:
        return [str(p) for p in _list_segment_dirs(SEGMENTS_DIR)]
    
    def index_folder(self, docs_dir: Path, threads: int = 0) -> int:
        if not docs_dir.is_dir():
            raise FileNotFoundError(f"Docs folder not found: {docs_dir}")

        paths: List[str] = []
        for p in sorted(docs_dir.iterdir()):
            if not p.is_file() or p.suffix.lower() != ".txt":
                continue
            full = str(p.resolve())
            paths.append(full)

        if not paths:
            return 0

        to_index: List[Tuple[str,str]] = []
        for path in paths:
            data = Path(path).read_bytes()
            h = hashlib.sha256(data).hexdigest()

            old_hash = self.db.get_hash_by_path(path)
            if old_hash is not None and str(old_hash) == h:
                continue
            to_index.append((path,h))

        if not to_index:
            return 0

        start_doc_id = self.db.alloc_doc_id_block(len(to_index))
        path_for_segment: List[str] = []
        for i, (path, h) in enumerate(to_index):
            old_Id = self.db.get_docid_by_path(path)
            new_Id = start_doc_id + i
            title = Path(path).name
            if old_Id is not None and not self.db.is_deleted(old_Id):
                old_Id = int(old_Id)
            else:
                old_Id = None
            self.db.upsert_doc(path, title, int(new_Id), h, old_Id)
            path_for_segment.append(path)

        build_fn = self._require("build_segment_parallel")

        try:
            build_fn(path_for_segment, int(start_doc_id), str(SEGMENTS_DIR), int(threads))
        except TypeError:
            build_fn(path_for_segment, int(start_doc_id), str(SEGMENTS_DIR))

        return len(path_for_segment)

    def delete_path(self, path: str) -> int:
        p = Path(path)
        if not p.is_absolute():
            p = (self.project_root / p).resolve()
        norm = str(p)

        doc_id = self.db.get_docid_by_path(norm)
        if doc_id is None:
            return 0
        if not self.db.is_deleted(doc_id):
            self.db.tombstone(doc_id)
        try:
            delete_fn = self._require("delete_by_path_all")
            delete_fn(str(SEGMENTS_DIR), norm)
        except Exception:
            pass
        return 1

    def merge_smallest(self) -> int:
        merge_fn = self._require("merge_smallest")
        return int(merge_fn(str(SEGMENTS_DIR), self.db))

    def search(self, query: str, k: int = 10, k1: float = 1.2, b: float = 0.75) -> List[SearchHit]:
        segs = self._segment_paths()
        if not segs:
            return []
        
        search_fn = self._require("search_bm25")
        try:
            raw = search_fn(segs, str(query), int(k), float(k1), float(b))
        except TypeError:
            raw = search_fn(segs, str(query), int(k))

        out: List[SearchHit] = []
        for score, doc_id in raw:
            doc_id = int(doc_id)
            if self.db.is_deleted(doc_id):
                continue

            meta = self.db.get_docmeta(doc_id)
            if meta is None:
                continue
            title, path = meta
            current = self.db.get_docid_by_path(path)
            if current is not None and int(current) != doc_id:
                continue

            out.append(SearchHit(float(score), doc_id, str(title), str(path)))
            if len(out) >= k:
                break

        return out

    def stats(self) -> None:
        segs = _list_segment_dirs(SEGMENTS_DIR)
        print(f"Segments: {len(segs)}")
        print(f"Next docId: {self.db.peek_next_doc_id()}")
        print(f"Docs dir: {DOCS_DIR}")
        print()

        for i, seg in enumerate(segs, start=1):
            meta_path = seg / "meta.json"
            doc_count = "?"
            created = "?"
            if meta_path.exists():
                try:
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    doc_count = meta.get("docCount", "?")
                    created = meta.get("createdAtUnix", meta.get("created_at_unix", "?"))
                except Exception:
                    pass
            print(f"  {i}. {seg.name}  docCount={doc_count}  createdAtUnix={created}")
    
    def check_bindings(self) -> None:
        expected = [
            "build_segment_parallel",
            "search_bm25",
            "delete_by_path_all",
            "merge_smallest",
        ]
        available = [n for n in dir(searchcore) if not n.startswith("_")]
        print("searchcore functions available:")
        for n in available:
            print("  ", n)
        print()

        missing = [n for n in expected if not hasattr(searchcore, n)]
        if missing:
            raise RuntimeError(f"Missing expected bindings: {missing}")
        print("All expected bindings found")


def main() -> None:
    engine = EngineController(PROJECT_ROOT)

    print("CWD:", os.getcwd())
    print("Project root:", PROJECT_ROOT)
    print("Segments dir:", SEGMENTS_DIR)
    print("Docs dir:", DOCS_DIR)
    print("\nCommands:")
    print("  :index [threads]        -> index docs/ into a NEW segment (update existing files)")
    print("  :stats                  -> show segment stats")
    print("  :deletepath <path>      -> tombstone by file path across segments")
    print("  :merge                  -> merge two smallest segments (drops deletes)")
    print("  :check                  -> verify C++ bindings are present")
    print("  :quit                   -> exit")
    print("Or type a search query.\n")

    while True:
        q = input("search> ").strip()
        if not q:
            continue

        if q == ":quit":
            break

        if q.startswith(":index"):
            parts = q.split()
            threads = int(parts[1]) if len(parts) > 1 else 0
            n = engine.index_folder(DOCS_DIR, threads=threads)
            print(f"Indexed {n} docs into a new segment.")
            continue

        if q == ":stats":
            engine.stats()
            continue

        if q.startswith(":deletepath "):
            path = q.split(" ", 1)[1].strip()
            affected = engine.delete_path(path)
            print(f"deleted in {affected} segment(s)" if affected > 0 else "path not found / already deleted")
            continue

        if q == ":merge":
            live_docs = engine.merge_smallest()
            print("No merge performed (need >=2 segments)." if live_docs == 0 else f"Merged; new segment liveDocs={live_docs}.")
            continue

        if q == ":check":
            engine.check_bindings()
            continue

        results = engine.search(q, k=10)
        if not results:
            print("(no matches)")
            continue

        for hit in results:
            print(f"  {hit.score:.3f}  {hit.doc_id}  {hit.title}  [{hit.path}]")
        print()


if __name__ == "__main__":
    main()