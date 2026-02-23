from collections import Counter, defaultdict
from dataclasses import dataclass
import math
import os
import pickle
import re
from typing import Dict, List, Tuple


@dataclass
class Document:
    id: int
    title: str
    path: str
    text: str

WORD = re.compile(r"[a-z0-9]+")

def tokenize(s: str) -> List[str]:
    return WORD.findall(s.lower())

class SearchEngine:
    def __init__(self):
        self.docs: Dict[int, Document] = {}
        self.postings: Dict[str, Dict[int,int]] = defaultdict(dict)
        self.docLen: Dict[int,int] = {}
        self.nextId: int = 1

    def add(self, title: str, path: str, text: str) -> int:
        docId = self.nextId
        self.nextId += 1
        doc = Document(id=docId,title=title,path=path,text=text)
        self.docs[docId] = doc
        tokens = tokenize(title + " " + text)
        self.docLen[docId] = len(tokens)
        freqs = Counter(tokens)
        for term, freq in freqs.items():
            self.postings[term][docId] = freq

    def indexFolder(self, folder: str) -> int:
        count = 0
        for name in sorted(os.listdir(folder)):
            if not name.lower().endswith(".txt"):
                continue
            path = os.path.join(folder,name)
            with open(path,"r",encoding="utf-8",errors="ignore") as f:
                text = f.read()
            title = os.path.splitext(name)[0].replace("_"," ")
            self.add(title,path,text)
            count += 1
        return count
    
    def search(self, query: str, k: int = 10) -> List[Tuple[float,Document]]:
        terms = tokenize(query)
        if not terms:
            return []
        n = len(self.docs) or 1

        candidates = set()
        for t in terms:
            candidates.update(self.postings.get(t, {}).keys())
        
        results: List[Tuple[float,int]] = []
        for id in candidates:
            score = 0.0
            for t in terms:
                freq = self.postings.get(t,{}).get(id,0)
                if freq == 0:
                    continue
                docFreq = len(self.postings.get(t,{}))
                rare = math.log((n+1) / (docFreq + 1)) + 1.0
                score += freq * rare
            score /= math.sqrt(self.docLen.get(id,1))
            results.append((score,id))
        results.sort(reverse=True, key=lambda x: x[0])
        return [(score, self.docs[id]) for score, id in results[:k]]
    
    def save(self, path:str) -> None:
        with open(path,"wb") as f:
            pickle.dump(self,f)
    
    @staticmethod
    def load(path: str) -> "SearchEngine":
        with open(path,"rb") as f:
            obj = pickle.load(f)
        if not isinstance(obj,SearchEngine):
            raise TypeError("File did not contain a SearchEngine")
        return obj

def main():
    docsFolder = "docs"
    indexFile = "index.pkl"

    if os.path.exists(indexFile):
        engine = SearchEngine.load(indexFile)
    else:
        
        engine = SearchEngine()
        n = engine.indexFolder(docsFolder)
        if not os.path.isdir(docsFolder):
            raise FileNotFoundError("Missing folder")
        engine.save(indexFile)
    
    while True:
        q = input("search ").strip()
        if not q:
            continue
        if q == ":quit":
            break
        results=engine.search(q,k=10)
        if not results:
            print("no matches")
            continue
        for score, doc in results:
            print(f"  {score:.3f}   {doc.id}    {doc.title} [{doc.path}]")
        print()

if __name__ == "__main__":
    main()