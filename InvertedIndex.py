from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")

class InvertedIndex(MRJob):

    def mapper(self, _, line):
        doc_id, text = line.split(": ", 1)
        
        for word in WORD_RE.findall(text):
            yield word, doc_id

    def reducer(self, word, doc_ids):
        yield word, list(set(doc_ids))

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

if __name__ == '__main__':
    InvertedIndex.run()

