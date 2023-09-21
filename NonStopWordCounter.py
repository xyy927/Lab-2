from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")

class NonStopWordCount(MRJob):

    STOPWORDS = set(["the", "and", "of", "a", "to", "in", "is", "it", "these", "this"])

    def mapper_get_words(self, _, line):
        for word in WORD_RE.findall(line):
            word = word.lower()
            if word not in self.STOPWORDS:
                yield (word, 1)

    def combiner_count_words(self, word, counts):
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        yield (word, sum(counts))

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words)
        ]

if __name__ == '__main__':
    NonStopWordCount.run()



