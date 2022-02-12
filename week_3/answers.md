### Question 1: What is count for fhv vehicles data for year 2019?
42084899 records

### Question 2: How many distinct dispatching_base_num we have in fhv for 2019
792 unique dispatching_base_num values

### Question 3: Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num
Partition by dropoff_datetime and cluster by dispatching_base_num

### Question 4: What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279
Count: 26558
Estimated: 400.1 MiB
Actual: 130.5 MiB
Count: 26558, Estimated data processed: 400 MB, Actual data processed: 155 MB

### Question 5: What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag
Partition by SR_Flag and cluster by dispatching_base_num

### Question 6: What improvements can be seen by partitioning and clustering for data size less than 1 GB
It's either no improvements or can be worse due to the metadata associated with the table.  