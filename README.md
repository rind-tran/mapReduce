# MapReduce Project - NYC TLC yellow taxi

![image](images/Telsa-model-3-yellow-taxi.jpg)

The data set can be downloaded from these links:

[yellow_tripdata_2017-01.csv](https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-01.csv)  
[yellow_tripdata_2017-02.csv](https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-02.csv)  
[yellow_tripdata_2017-03.csv](https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-03.csv)  
[yellow_tripdata_2017-04.csv](https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-04.csv)  
[yellow_tripdata_2017-05.csv](https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-05.csv)  
[yellow_tripdata_2017-06.csv](https://nyc-tlc-upgrad.s3.amazonaws.com/yellow_tripdata_2017-06.csv) 

Data dictionary:

https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

----
## Task  
The **Big Data** technology used in this project:
- AWS EMR instance including: Hadoop, Apache HBase, Apache Sqoop.
- AWS RDS.

### Data Ingestion Tasks:  
**Task 1.** [Create an RDS instance](https://cdn.upgrad.com/uploads/production/08f82196-b083-426a-ab25-1eb052c43683/Documentation%2B-%2BWorking%2Bwith%2BRDS.pdf) in your AWS account and upload the data from two files (yellow_tripdata_2017-01.csv & yellow_tripdata_2017-02.csv) from the dataset. Make sure to create an appropriate schema for the data sets before uploading them to RDS.  

**Task 2.** Use Sqoop command to ingest the data from RDS into the HBase Table.  

**Task 3.** Bulk import data from next two files in the dataset on your EMR cluster to your HBase Table using the relevant codes.
_Note: For the above task 3, you just need to import data from the subsequent 2 csv files_

### MapReduce Tasks:  
**Task 4.** Write MapReduce codes to perform the tasks using the files you’ve downloaded on your EMR Instance:
- Which vendors have the most trips, and what is the total revenue generated by that vendor?
- Which pickup location generates the most revenue? 
- What are the different payment types used by customers and their count? The final results should be in a sorted format.
- What is the average trip time for different pickup locations?
-  Calculate the average tips to revenue ratio of the drivers for different pickup locations in sorted format.
- How does revenue vary over time? Calculate the average trip revenue per month - analysing it by hour of the day (day vs night) and the day of the week (weekday vs weekend).
_NOTE: It's recommended to use MRJob for completing the MapReduce taks above._
