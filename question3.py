from mrjob.job import MRJob
from mrjob.step import MRStep


class MyMapReduce(MRJob):
 
    # Our mapper takes a fragment of text as an input and produces a list of (key, value)
    # The key is payment_type and the value is 1, indicating one count of the payment_type has been read
    def mapper(self, _, line):
        line = line.split(',')
        if line[0] != "VendorID":
            yield(line[9], 1)
 
    # Our reducer takes a group of (key, value) where key = payment_type, and produces a final (key, value)
    # The key is a payment_type and its value is the number of occurrences of the payment_type
    def reducer(self, payment_type, counts):
        yield None, (sum(counts), payment_type)

    def sort_counts(self, _, pair):
        for counts, payment_type in sorted(pair, reverse=True):
            yield(counts, payment_type)

    def steps(self):
        return [MRStep(mapper=self.mapper,
                reducer=self.reducer),
                MRStep(reducer=self.sort_counts)]


if __name__ == '__main__':
    MyMapReduce.run()
