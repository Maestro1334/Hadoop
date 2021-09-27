from mrjob.job import MRJob
from mrjob.step import MRStep


class RatingsSum(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_sum_ratings),
            MRStep(reducer=self.reducer_sort_ratings)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, int(rating)

    def reducer_sum_ratings(self, key, values):
        yield None, (sum(values), key)

    def reducer_sort_ratings(self, _, pair):
        sorted_pairs = sorted(pair, reverse=True)
        for pair in sorted_pairs:
            yield pair


if __name__ == '__main__':
    RatingsSum.run()
