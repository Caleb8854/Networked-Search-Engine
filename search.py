from collections import Counter, defaultdict
from dataclasses import dataclass
import json
import math
import os
import pickle
import re
import shutil
import time
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class Document:
    id: int
    title: str
    path: str
    text: str

WORD = re.compile(r"[a-z0-9]+")

def tokenize(s: str) -> List[str]:
    return WORD.findall(s.lower())

class Segment:
    def __init__(self, segDir: str):
        self.segDir = segDir
        self._docs: Optional[Dict[int, Document]] = None
        self._postings: Optional[Dict[str, Dict[int, int]]] = None
        self._doclen: Optional[Dict[int, int]] = None
        self._termdf: Optional[Dict[str, int]] = None
        self._meta: Optional[Dict[str, Any]] = None

    def loadPickle(self, name: str):
        path = os.path.join(self.segDir, name)
        with open(path, "rb") as f:
            return pickle.load(f)

    def loadJson(self, name: str):
        path = os.path.join(self.segDir, name)
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    @property
    def docs(self) -> Dict[int, Document]:
        if self._docs is None:
            self._docs = self.loadPickle("docs.pkl")
        return self._docs

    @property
    def postings(self) -> Dict[str, Dict[int, int]]:
        if self._postings is None:
            self._postings = self.loadPickle("postings.pkl")
        return self._postings

    @property
    def doclen(self) -> Dict[int, int]:
        if self._doclen is None:
            self._doclen = self.loadPickle("doclen.pkl")
        return self._doclen

    @property
    def termdf(self) -> Dict[str, int]:
        if self._termdf is None:
            self._termdf = self.loadPickle("termdf.pkl")
        return self._termdf

    @property
    def meta(self) -> Dict[str, Any]:
        if self._meta is None:
            self._meta = self.loadJson("meta.json")
        return self._meta

class SegmentWriter:
    def __init__(self):
        self.docs: Dict[int, Document] = {}
        self.postings: Dict[str, Dict[int, int]] = defaultdict(dict)
        self.doclen: Dict[int, int] = {}

    def addDoc(self, doc: Document) -> None:
        self.docs[doc.id] = doc

        tokens = tokenize(doc.title + " " + doc.text)
        self.doclen[doc.id] = len(tokens)

        freqs = Counter(tokens)
        for term, freq in freqs.items():
            self.postings[term][doc.id] = freq

    def flush(self, segDir: str) -> None:
        os.makedirs(segDir, exist_ok=False)

        termdf = {term: len(docMap) for term, docMap in self.postings.items()}

        with open(os.path.join(segDir, "docs.pkl"), "wb") as f:
            pickle.dump(self.docs, f)

        with open(os.path.join(segDir, "postings.pkl"), "wb") as f:
            pickle.dump(dict(self.postings), f)

        with open(os.path.join(segDir, "doclen.pkl"), "wb") as f:
            pickle.dump(self.doclen, f)

        with open(os.path.join(segDir, "termdf.pkl"), "wb") as f:
            pickle.dump(termdf, f)

        meta = {
            "docCount": len(self.docs),
            "created_at_unix": int(time.time()),
        }
        with open(os.path.join(segDir, "meta.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)

class SearchEngine:
    def __init__(self, root: str = "segments"):
        self.root = root
        self.manifestPath = os.path.join(self.root, "manifest.json")
        os.makedirs(self.root, exist_ok=True)
        self.manifest = self.loadOrInitManifest()
        self.segments: List[Segment] = [Segment(os.path.join(self.root,segName)) for segName in self.manifest["segments"]]
        self.seen = set(self.manifest["seen"])
        self.deleted = set(self.manifest["deletedDocIds"])
        self.buildDocToSeg()
        self.pathToDoc = dict(self.manifest["pathToDocId"])
        self.docToPath = {v: k for k, v in self.pathToDoc.items()}
        
    def loadOrInitManifest(self) -> Dict[str, Any]:
        if os.path.exists(self.manifestPath):
            with open(self.manifestPath, "r", encoding="utf-8") as f:
                manifest = json.load(f)
                if "pathToDocId" not in manifest:
                    manifest["pathToDocId"] = {}
                if "deletedDocsId" not in manifest:
                    manifest["deletedDocsId"] = []
                if "seen" not in manifest:
                    manifest["seen"] = []
                if "nextSegmentId" not in manifest:
                    manifest["nextSegmentId"] = 1
                return manifest
        manifest = {"version": 1, "segments": [], "nextId": 1, "nextSegmentId": 1, "totalDocs": 0, "seen": [], "deletedDocIds": [], "pathToDocId": {}}
        self.writeManifest(manifest)
        return manifest
    
    def writeManifest(self, manifest: Dict[str, Any]) -> None:
        tmp = self.manifestPath + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        os.replace(tmp, self.manifestPath)

    def newSegmentName(self) -> str:
        segId = self.manifest["nextSegmentId"]
        self.manifest["nextSegmentId"] += 1
        return f"seg_{segId:06d}"
    
    def buildDocToSeg(self) -> None:
        self.docToSeg: Dict[int,int] = {}
        for i, seg in enumerate(self.segments):
            for docId in seg.docs.keys():
                if docId in self.deleted:
                    continue
                self.docToSeg[docId] = i

    def globalDf(self, term: str) -> int:
        docFreq = 0
        for seg in self.segments:
            docFreq += seg.termdf.get(term,0)
        return docFreq
    
    def docCountInSegment(self, seg: Segment) -> int:
        live = 0
        for docId in seg.docs.keys():
            if docId not in self.deleted:
                live += 1
        return live

    def listSegments(self) -> List[Tuple[str,int]]:
        out = []
        for seg in self.segments:
            segName = os.path.basename(seg.segDir)
            out.append((segName, int(seg.meta["docCount"])))
        return out
    
    def mergeSmallest(self) -> int:
        if len(self.segments) < 2:
            return 0
        segInfos = []
        for seg in self.segments:
            segInfos.append((int(seg.meta["docCount"]),seg))
        segInfos.sort(key=lambda x: x[0])
        a = segInfos[0][1]
        b = segInfos[1][1]
        return self.mergeSegments(os.path.basename(a.segDir),os.path.basename(b.segDir))
    
    def mergeSegments(self, segA: str, segB: str) -> int:
        a = None
        b = None
        for seg in self.segments:
            name = os.path.basename(seg.segDir)
            if name == segA:
                a = seg
            elif name == segB:
                b = seg
        if a is None or b is None:
            raise ValueError(f"Could not find both segments: {segA}, {segB}")
        if segA == segB:
            raise ValueError("Cannot merge the same segment")
            
        docs = {}
        for dId, d in a.docs.items():
            if dId not in self.deleted:
                docs[dId] = d
        for dId, d in b.docs.items():
            if dId not in self.deleted:
                docs[dId] = d

        doclen = {}
        for dId, dl in a.doclen.items():
            if dId not in self.deleted and dId in docs:
                doclen[dId] = dl
        for dId, dl in b.doclen.items():
            if dId not in self.deleted and dId in docs:
                doclen[dId] = dl

        mergedPostings = defaultdict(dict)
        for term, docMap in a.postings.items():
            for dId, tf in docMap.items():
                if dId not in self.deleted:
                    mergedPostings[term][dId] = tf
        for term, docMap in b.postings.items():
            for dId, tf in docMap.items():
                if dId not in self.deleted:
                    mergedPostings[term][dId] = tf

        mergedWriter = SegmentWriter()
        mergedWriter.docs = docs
        mergedWriter.doclen = doclen
        mergedWriter.postings = mergedPostings

        mergedName = self.newSegmentName()
        mergedDir = os.path.join(self.root, mergedName)
        mergedWriter.flush(mergedDir)

        mergedCount = len(docs)

        self.manifest["segments"] = [s for s in self.manifest["segments"] if s not in (segA, segB)]
        self.manifest["segments"].append(mergedName)

        self.segments = [seg for seg in self.segments if os.path.basename(seg.segDir) not in (segA, segB)]
        self.segments.append(Segment(mergedDir))

        self.buildDocToSeg()

        self.manifest["totalDocs"] = len(self.docToSeg)

        self.writeManifest(self.manifest)

        shutil.rmtree(os.path.join(self.root, segA), ignore_errors=True)
        shutil.rmtree(os.path.join(self.root, segB), ignore_errors=True)

        return mergedCount
    
    def autoMerge(self, maxSegments: int = 10) -> None:
        while len(self.segments) > maxSegments:
            mergedDocs = self.mergeSmallest()
            if mergedDocs == 0:
                break
        return
    
    def compactOne(self) -> int:
        if len(self.segments) != 1:
            return 0

        seg = self.segments[0]
        segName = os.path.basename(seg.segDir)

        docs = {dId: d for dId, d in seg.docs.items() if dId not in self.deleted}
        doclen = {dId: dl for dId, dl in seg.doclen.items() if dId in docs}

        mergedPostings = defaultdict(dict)
        for term, docMap in seg.postings.items():
            for dId, tf in docMap.items():
                if dId in docs:
                    mergedPostings[term][dId] = tf

        w = SegmentWriter()
        w.docs = docs
        w.doclen = doclen
        w.postings = mergedPostings

        newName = self.newSegmentName()
        newDir = os.path.join(self.root, newName)
        w.flush(newDir)

        self.manifest["segments"] = [newName]
        self.segments = [Segment(newDir)]
        self.buildDocToSeg()
        self.manifest["totalDocs"] = len(self.docToSeg)
        self.writeManifest(self.manifest)

        shutil.rmtree(os.path.join(self.root, segName), ignore_errors=True)

        return len(docs)
    
    def persistDelete(self) -> None:
        self.manifest["deletedDocIds"] = list(self.deleted)
        self.manifest["pathToDocId"] = self.pathToDoc
        self.manifest["totalDocs"] = len(self.docToSeg)
        self.manifest["seen"] = list(self.seen)
        self.writeManifest(self.manifest)

    def deleteDoc(self, docId: int) -> bool:
        if docId in self.deleted:
            return False
        if docId not in self.docToSeg:
            return False
        self.deleted.add(docId)
        path = self.docToPath.get(docId)
        if path is not None:
            if path in self.seen:
                self.seen.remove(path)
            if path in self.pathToDoc:
                del self.pathToDoc[path]
            del self.docToPath[docId]
        self.buildDocToSeg()
        self.persistDelete()
        return True
    
    def deletePath(self, path: str) -> bool:
        path = os.path.normpath(path)
        docId = self.pathToDoc.get(path)
        if docId is None:
            return False
        return self.deleteDoc(int(docId))

    def indexFolder(self, folder: str) -> int:
        if not os.path.isdir(folder):
            raise FileNotFoundError(f"Folder not found {folder}")
        writer = SegmentWriter()
        added = 0

        for name in sorted(os.listdir(folder)):
            if not name.lower().endswith(".txt"):
                continue
            path = os.path.join(folder, name)
            if path in self.seen:
                continue
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                text = f.read()
            title = os.path.splitext(name)[0].replace("_", " ")
            docId = self.manifest["nextId"]
            self.pathToDoc[path] = docId
            self.docToPath[docId] = path
            if docId in self.deleted:
                self.deleted.remove(docId)
                continue
            else:
                doc = Document(id=docId,title=title,path=path,text=text)
                writer.addDoc(doc)
            self.manifest["nextId"] += 1
            self.seen.add(path)
            added += 1

        if added == 0:
            return 0

        segName = self.newSegmentName()
        segDir = os.path.join(self.root, segName)

        writer.flush(segDir)

        self.manifest["segments"].append(segName)
        self.manifest["totalDocs"] += added
        self.manifest["seen"] = list(self.seen)
        self.manifest["pathToDocId"] = self.pathToDoc
        self.manifest["deletedDocIds"] = list(self.deleted)
        self.writeManifest(self.manifest)

        self.segments.append(Segment(segDir))

        self.buildDocToSeg()
        self.autoMerge(maxSegments=10)

        return added
    
    def search(self, query: str, k: int = 10) -> List[Tuple[float,Document]]:
        terms = tokenize(query)
        if not terms:
            return []
        n = self.manifest["totalDocs"] or 1

        candidates = set()
        for seg in self.segments:
            for t in terms:
                candidates.update(seg.postings.get(t, {}).keys())
        
        results: List[Tuple[float,int,Document]] = []
        for docId in candidates:
            if docId in self.deleted:
                continue
            
            score = 0.0

            segIndex = self.docToSeg.get(docId)
            if segIndex is None:
                continue

            seg = self.segments[segIndex]
            doc = seg.docs[docId]
            doclen = seg.doclen.get(docId, 1)

            for t in terms:
                tf = seg.postings.get(t, {}).get(docId, 0)
                if tf == 0:
                    continue
                df = self.globalDf(t)
                idf = math.log((n + 1) / (df + 1)) + 1.0
                score += tf * idf
            score /= math.sqrt(doclen)
            results.append((score,docId,doc))
        results.sort(reverse=True, key=lambda x: x[0])
        return [(score, doc) for score, _, doc in results[:k]]
    
def main():
    docs_folder = "docs"
    engine = SearchEngine(root="segments")

    print("CWD:", os.getcwd())
    print("Segments:", len(engine.segments), "Total docs:", engine.manifest["totalDocs"])

    print("\nCommands:")
    print("  :index      -> index docs/ as a NEW segment")
    print("  :stats      -> show segment stats")
    print("  :quit       -> exit")
    print("  :merge      -> merge 2 smallest segments")
    print("  :deleteid <id>       -> delete docId")
    print("  :deletepath <path>   -> delete by file path")
    print("  :deleted             -> show deleted count")
    print("  :gc                  -> force merges until 1 segment (reclaims deletes)")
    print("Or type a search query.\n")

    while True:
        q = input("search> ").strip()
        if not q:
            continue

        if q == ":quit":
            break

        if q == ":index":
            n = engine.indexFolder(docs_folder)
            print(f"Indexed {n} documents into a new segment. Total docs now: {engine.manifest['totalDocs']}")
            continue
        if q == ":stats":
            print(f"Total live docs: {len(engine.docToSeg)}")
            print(f"Total deleted docs: {len(engine.deleted)}")
            print(f"Segment count: {len(engine.segments)}\n")
            for i, seg in enumerate(engine.segments, start=1):
                segName = os.path.basename(seg.segDir)
                physical = len(seg.docs)
                live = 0
                for docId in seg.docs.keys():
                    if docId not in engine.deleted:
                        live += 1
                print(f"  {i}. {segName}  physical={physical}  live={live}")
            continue
        if q == ":merge":
            n = engine.mergeSmallest()
            if n:
                print(f"Merged segments into a new one with {n} docs.")
            continue
        if q.startswith(":deleteid "):
            try:
                docId = int(q.split(" ", 1)[1].strip())
            except ValueError:
                print("Usage: :deleteid <number>")
                continue
            ok = engine.deleteDocId(docId)
            print("deleted" if ok else "not found/already deleted")
            continue

        if q.startswith(":deletepath "):
            path = q.split(" ", 1)[1].strip()
            ok = engine.deletePath(path)
            print("deleted" if ok else "path not found")
            continue

        if q == ":deleted":
            print(f"Deleted docs: {len(engine.deleted)}")
            continue

        if q == ":gc":
            merges = 0
            while len(engine.segments) > 1:
                engine.mergeSmallest()
                merges += 1
            if len(engine.segments) == 1:
                engine.compactOne()
            print(f"GC done. merges={merges}, segments={len(engine.segments)}")
            continue

        results = engine.search(q, k=10)
        if not results:
            print("(no matches)")
            continue

        for score, doc in results:
            print(f"  {score:.3f}  {doc.id}  {doc.title}  [{doc.path}]")
        print()

if __name__ == "__main__":
    main()