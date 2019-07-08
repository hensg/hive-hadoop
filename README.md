# Hive 1.1.0 Hadoop 2.6.0

Small project that loads some data into hadoop and perform some aggregations using Hive. Docker was used to setup a local hive-hadoop cluster.


### Requirements:
- Docker version 18.09.02

### Project Structure:
Infra:
- Dockerfile: to build an image with Hive 1.1.0 Hadoop 2.6.0 and with configuration files (hadoop-conf/hive-conf)
- start-hadoop-cluster.sh: bash helper to build and start hadoop cluster with docker
- stop-hadoop-cluster.sh: bash helper to stop hadoop cluster
- hive-docker-cli.sh: a hive docker cli for hive shell


Data:
- hive-script.sql: contains HiveQL to load data, count and perform aggregations to answer Hive data analysis questions
- data.dat: sample data to analyze (hotel search)
- hive-script-loader.sh: first copy data.dat, hive-script.sql and UDF jar to container. After that, put them into HDFS and finally run hive-script.sql in Hive
- hive-udf: project that contains an UDF to find cheapest hotel prices that offers breakfast.
- hive-udf-1.0-SNAPSHOT.jar: package created with maven, loaded into Hive by hive-script-loader.sh

#### How to run:

`$ ./start-hadoop-cluster.sh`

`$ ./hive-script-loader.sh`

------------------------

### Hadoop data loading & Hive data analysis:

A. Load the data into Hadoop, and perform a count of the records. List the steps you took to get the data in and to make the count.
1. Loaded data into hadoop "hdfs dfs -put -f file:///tmp/data.dat /tmp/search_results.dat" (Using namenode image that has hadoop configured)
2. Loaded data into a temporary table using "load data inpath '/tmp/search_results.dat' into table default.tmp_search_results;"
3. Counting data from temporary table using "select count(*) from default.tmp_search_results"
4. Count: 1012; Result: Sucess; Time taken: 24seconds; Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 4.22 sec   HDFS Read: 1112267 HDFS Write: 4; 
5. Created a table named "default.search_results" partitioned by "date_of_search" (events, so... day would be a good granularity, but it depends on amount of data)
6. Inserted into "default.search_results" only good data (where unix_time is not null and userid is not null)
7. Counting data from "select count(*) from default.search_results"
8. Count: 996; Time taken: 23seconds Stage-Stage-1: Map:1 Reduce:1 Cumulative CPU: 4.22 sec HDFS Read: 1112267 HDFS Write: 4 


B. Execute a query to find per advertiser and hotel the cheapest price that was offered. Provide the query, and the result you got.
HQL: 
with advertiser_and_hotel_ranked_by_eurocents as (
select
  advertiser,
  hotelid,
  deal.eurocents,
  rank() over (partition by advertiser, hotelid order by eurocents) as rank
from
  default.search_results
  lateral view explode(hotelresults) t1 as hotelid, advertisers
  lateral view explode(advertisers.advertisers) t2 as advertiser, deals
  lateral view inline(deals) deal
)
select
  advertiser,
  hotelid,
  eurocents as cheapest_price
from
  default.advertiser_and_hotel_ranked_by_eurocents
where
  rank = 1
limit 500;

Result:
Amoma 6032 3804
Amoma 6033 3815
Amoma 6035 3919
Amoma 6036 3839
Amoma 7045 3847
Amoma 7046 3857
Amoma 7047 3805
Amoma 8001 3808
Amoma 8002 3841
Amoma 9089 3804
Amoma 9090 3927
Amoma 9091 3853
Amoma 9092 3893
Destinia 6032 3814        
Destinia 6033 3808
Destinia 6035 3879
Destinia 6036 3823
Destinia 7045 3805
Destinia 7046 3812
Destinia 7047 3823
Destinia 8001 3800
Destinia 8002 3802
Destinia 9089 3820
Destinia 9090 3839
Destinia 9091 3871
Destinia 9092 3807
Mercure 6032 3804
Mercure 6033 3810
Mercure 6035 3854
Mercure 6036 3965
Mercure 7045 3879
Mercure 7046 3836
Mercure 7047 3850
Mercure 8001 3829
Mercure 8002 3831
Mercure 9089 3863
Mercure 9090 3802
Mercure 9091 3875
Mercure 9092 3910
Tui.com 6032 3893
Tui.com 6033 3840
Tui.com 6035 3970
Tui.com 6036 3884                                                             
Tui.com 7045 3839                                                               
Tui.com 7046 3803                                                              
Tui.com 7047 3832                                   
Tui.com 8001 3892  
Tui.com 8002 3807
Tui.com 9089 3859
Tui.com 9090 3837
Tui.com 9091 3819
Tui.com 9092 3824
booking.com 6032 3852
booking.com 6033 3845 
booking.com 6036 3855
booking.com 7045 3847
booking.com 7046 3961
booking.com 7047 3832
booking.com 8001 3835
booking.com 8002 3817
booking.com 9089 3814
booking.com 9090 3884
booking.com 9091 3846
booking.com 9092 3807
expedia 6032 3924
expedia 6033 3819
expedia 6035 3804
expedia 6036 3813
expedia 7045 3812
expedia 7046 3814
expedia 7047 3945
expedia 8001 3833
expedia 8002 3869
expedia 9089 3819
expedia 9090 3816
expedia 9091 3802
expedia 9092 3817
Time taken: 20.709 seconds, Fetched: 78 row(s)


C. For each search generate a list containing the cheapest price per hotel that offers breakfast. Again, please provide the query you used and the result.
HQL: 
with search_cheapest_price_that_offers_breakfast as (
  select
    userid,
    unix_time,
    hotelid,
    min(deal.eurocents) as cheapest_price_by_hotel
  from
    default.search_results
    lateral view explode(hotelresults) t1 as hotelid, advertisers
    lateral view explode(advertisers.advertisers) t2 as advertiser, deals
    lateral view inline(deals) deal 
  where
    deal.breakfast
  group by
    userid,
    unix_time,
    hotelid
)
select
  userid,
  unix_time,
  collect_list(cheapest_price_by_hotel) as cheapest_prices
from
  search_cheapest_price_that_offers_breakfast
group by
  userid,
  unix_time
limit 100;

Results: no results (there is no hotel that offers breakfast)
Time taken: 23.674 seconds


D. Generate the list from "task 3" with more efficiency (think about UDFs!). Provide all resources to understand your solution, and measure the difference in execution times.
HQL:
ADD JAR hdfs://namenode:8020/udfs/hive-udf-1.0-SNAPSHOT.jar;
drop function if exists cheapest_prices_per_hotel_breakfast;
create function cheapest_prices_per_hotel_breakfast AS 'udf.CheapestPricesPerHotelBreakfast';

with search_cheapest_prices as (
select
  userid,
  unix_time,
  cheapest_prices_per_hotel_breakfast(hotelresults) as cheapest_prices
from
  default.search_results
)
select
  *
from
  search_cheapest_prices
where
  cheapest_prices is not null;
limit 100;

Results: no results;
Time taken: 0.198 seconds
