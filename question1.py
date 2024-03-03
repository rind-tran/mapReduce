from mrjob.job import MRJob

# This includes our definition of map and reduce functions
class MyMapReduce(MRJob):

    # The key is vendorID and the value is the total amount charged to passengers.
    def mapper(self, _, line):
        line = line.split(",")
        if (line[0] != "VendorID"):
            yield (line[0], float(line[16]))

    # Our reducer takes a group of (key, value) where key = word, and produces a final (key, value)
    def reducer(self, vendorID, total_amount):
        yield (vendorID, sum(total_amount))

if __name__ == '__main__':
    MyMapReduce.run()
