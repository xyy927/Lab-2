from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")

class BigramCount(MRJob):

    def mapper_get_bigrams(self, _, line):
        # Tokenize the line into words
        words = WORD_RE.findall(line)
        
        # Iterate through the words and yield bigrams
        for i in range(len(words) - 1):
            bigram = f"{words[i]},{words[i + 1]}"
            yield (bigram, 1)

    def combiner_count_bigrams(self, bigram, counts):
        # Sum up counts for each bigram we've seen so far
        yield (bigram, sum(counts))

    def reducer_count_bigrams(self, bigram, counts):
        # Yield the final count for each bigram
        yield (bigram, sum(counts))

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_bigrams,
                   combiner=self.combiner_count_bigrams,
                   reducer=self.reducer_count_bigrams)
        ]

if __name__ == '__main__':
    BigramCount.run()

