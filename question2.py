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
    # The key is a word and its value is the number of occurrences of the word
    def reducer(self, PULocationID, total_amount):
        my_dict = {}
        my_dict_count = {}
        if PULocationID not in my_dict:
            my_dict[PULocationID] = total_amount
            my_dict_count[PULocationID] = 1
        else:
            my_dict[PULocationID] += total_amount
            my_dict_count[PULocationID] += 1
        for item in my_dict:
            yield item, my_dict[item]/my_dict_count[item]


    def sort_by_amount(self, _, pair):
        sorted_pairs = sorted(pair, reverse=True)
        for pair in sorted_pairs:
            yield pair

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.sort_by_amount)
        ]

if __name__ == '__main__':
    MyMapReduce.run()
