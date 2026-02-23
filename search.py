from collections import Counter, defaultdict
from dataclasses import dataclass
import math
import re
from typing import Dict, List, Tuple


@dataclass
class Document:
    id: int
    title: str
    text: str

WORD = re.compile(r"[a-z0-9]+")

def tokenize(s: str) -> List[str]:
    return WORD.findall(s.lower())

class Search:
    def __init__(self):
        self.docs: Dict[int, Document] = {}
        self.postings: Dict[str, Dict[int,int]] = defaultdict(dict)
        self.docLen: Dict[int,int] = {}

    def add(self, doc: Document) -> None:
        self.docs[doc.id] = doc
        tokens = tokenize(doc.title + " " + doc.text)
        self.docLen[doc.id] = len(tokens)
        freqs = Counter(tokens)
        for term, freq in freqs.items():
            self.postings[term][doc.id] = freq
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