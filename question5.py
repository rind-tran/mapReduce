from mrjob.job import MRJob
from mrjob.step import MRStep


class MyMapReduce(MRJob):
 
    # Our mapper takes a fragment of text as an input and produces a list of (key, value)
    # The key is PULocationID and the value is (tip_amount, total_amount)
    def mapper(self, _, line):
        line = line.split(',')
        if line[0] != "VendorID":
            yield line[7], (line[13], line[16])

    # Our combiner calulate the tips to revenue ratio
    def combiner(self, locationID, pair):
        for tip_amount, total_amount in pair:
            tip_amount = float(tip_amount)
            total_amount = float(total_amount)
            if total_amount != 0:
                yield locationID, (float(tip_amount)/float(total_amount))
            else:
                yield locationID, 0

    # Our reducer takes (key, value) where key = locationID, and calculate average trip time
    # The key is a locationID and its value is the average trip time
    def reducer(self, locationID, ratio):
        count = 0
        total = 0
        for item in ratio:
            count += 1
            total += item
        yield None, (total/count, locationID)

    # Sort the result in step 2
    def sort_result(self, _, pair):
        for ratio, locationID in sorted(pair, reverse=True):
            yield (locationID, ratio)

    
    def steps(self):
        return [MRStep(mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer),
                MRStep(reducer=self.sort_result)]


if __name__ == '__main__':
    MyMapReduce.run()
