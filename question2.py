from mrjob.job import MRJob
from mrjob.step import MRStep

# This includes our definition of map and reduce functions
class MyMapReduce(MRJob):

    # The key is PULocationID (pickup location) and the value is the total amount charged to passengers.
    def mapper(self, _, line):
        line = line.split(",")
        if (line[0] != "VendorID"):
            yield (line[7], float(line[16]))

    # Our reducer takes a group of (key, value) where key = word, and produces a final (key, value)
    def reducer(self, location, total_amount):
        yield None, (sum(total_amount), location)


    def sort_by_amount(self, _, pair):
        sorted_pairs = sorted(pair, reverse=True)
        for revenue, location in sorted_pairs:
            yield location, revenue

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.sort_by_amount)
        ]

if __name__ == '__main__':
    MyMapReduce.run()
