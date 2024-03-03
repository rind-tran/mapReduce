from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

# This includes our definition of map and reduce functions
class MyMapReduce(MRJob):

    # The key is the composite of (month, weekday, hour) and the value is the total_amount charged to passengers.
    def mapper(self, _, line):
        line = line.split(",")
        if (line[0] != "VendorID"):
            pickup_date = datetime.strptime(line[1], '%Y-%m-%d %H:%M:%S')
            month = pickup_date.month
            hour = pickup_date.hour
            weekday = pickup_date.weekday()
            yield (month, weekday, hour), float(line[16])

    # Calculate the average revenue in reducer
    def reducer(self, composite_time, revenue):
        count = 0
        total = 0
        for item in revenue:
            count += 1
            total += item
        yield None, (composite_time, total/count)

    # Sort the result by month, weekday, hour
    def sort_by_amount(self, _, pair):
        sorted_pairs = sorted(pair, reverse=False)
        for composite_time, avg_revenue in sorted_pairs:
            yield composite_time, avg_revenue

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.sort_by_amount)
        ]

if __name__ == '__main__':
    MyMapReduce.run()
