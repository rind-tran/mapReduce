from mrjob.job import MRJob

# This includes our definition of map and reduce functions
class MyMapReduce(MRJob):

    # The key is vendorID and the value is (1: count ad 1 trip and the total amount charged to passengers.)
    def mapper(self, _, line):
        line = line.split(",")
        if (line[0] != "VendorID"):
            yield line[0], (1, float(line[16]))

    # Our reducer takes a group of (key, value) where key = word, and produces a final (key, value)
    def reducer(self, vendorID, total_amount):
        counts = 0
        total = 0
        for count, revenue in total_amount:
            counts += count
            total += revenue
        yield vendorID, (counts, round(total, 2))

if __name__ == '__main__':
    MyMapReduce.run()
