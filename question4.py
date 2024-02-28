from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime


class MyMapReduce(MRJob):
 
    # Our mapper takes a fragment of text as an input and produces a list of (key, value)
    # The key is PULocationID and the value is (tpep_pickup_datetime,tpep_dropoff_datetime), indicating one count of the payment_type has been read
    def mapper(self, _, line):
        line = line.split(',')
        if line[0] != "VendorID":
            yield line[7], (line[1], line[2])

    # Our combiner calulate the trip time
    def combiner(self, locationID, time):
        for pickup_datetime_str, dropoff_datetime_str in time:
            pickup_datetime = datetime.strptime(pickup_datetime_str, '%Y-%m-%d %H:%M:%S')
            dropoff_datetime = datetime.strptime(dropoff_datetime_str, '%Y-%m-%d %H:%M:%S')
            trip_time = dropoff_datetime - pickup_datetime
            trip_time_in_s = trip_time.total_seconds()
            yield (locationID, trip_time_in_s)

    # Our reducer takes (key, value) where key = locationID, and calculate average trip time
    # The key is a locationID and its value is the average trip time
    def reducer(self, locationID, time_list):
        count = 0
        total = 0
        for time in time_list:
            count += 1
            total += time
        yield (locationID, total/count)

    
    def steps(self):
        return [MRStep(mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer)]


if __name__ == '__main__':
    MyMapReduce.run()
