import happybase

# Variables
TABLE_NAME = "yellow_taxis_hbase"
BATCH_SIZE = 1000


# Define functions
def connect_hbase():
    print("Connecting to HBase...")
    hbase_con = happybase.Connection('localhost')
    hbase_con.open()
    print("Connected")
    return hbase_con


def disconnect_hbase(hbase_con):
    print("Disconnecting HBase...")
    try:
        hbase_con.close()
        print("Disconnected")
    except:
        print("Error trying disconnecting from HBase.")


# Insert CSV file to table in HBase
def batch_insert_data(hbase_con, filename):
    print("Starting batch insert of events...")
    file = open(filename, "r")
    table = hbase_con.table(TABLE_NAME)
    i = 0

    # Begin inserting
    with table.batch(batch_size=BATCH_SIZE) as b:
        for line in file:
			# Ignor the header line
            if i != 0:
				# Split value by delimiter ","
                row = line.strip().split(",")
                # import the result to table
                b.put(row[1]+row[2]+row[7], {'tlc_trip:VendorID': row[0],
                                             'tlc_trip:tpep_pickup_datetime': row[1],
                                             'tlc_trip:tpep_dropoff_datetime': row[2],
                                             'tlc_trip:passenger_count': row[3],
                                             'tlc_trip:trip_distance': row[4],
                                             'tlc_trip:RatecodeID': row[5],
                                             'tlc_trip:store_and_fwd_flag': row[6],
                                             'tlc_trip:PULocationID': row[7],
                                             'tlc_trip:DOLocationID': row[8],
                                             'tlc_trip:payment_type': row[9],
                                             'tlc_trip:fare_amount': row[10],
                                             'tlc_trip:extra': row[11],
                                             'tlc_trip:mta_tax': row[12],
                                             'tlc_trip:tip_amount': row[13],
                                             'tlc_trip:tolls_amount': row[14],
                                             'tlc_trip:improvement_surcharge': row[15],
                                             'tlc_trip:total_amount': row[16],
                                             'tlc_trip:congestion_surcharge': row[17],
                                             'tlc_trip:airport_fee': row[18]})
            i += 1
    # End of inserting
    file.close()
    print("batch insert done")


if __name__ == '__main__':
    # 1) Connect to HBaSe
    con = connect_hbase()
    # 2) Insert data
    batch_insert_data(con, 'yellow_tripdata_2017-03.csv')
    batch_insert_data(con, 'yellow_tripdata_2017-04.csv')
    # 3) Close connection
    disconnect_hbase(con)