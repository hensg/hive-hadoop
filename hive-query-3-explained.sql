------------------------------------------------------------------------------------
-- For each search generate a list containing the cheapest price per hotel that offers breakfast
-- But now using UDF
------------------------------------------------------------------------------------
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
  cheapest_prices is not null
limit 100;


--STAGE DEPENDENCIES:
--  Stage-0 is a root stage
--
--STAGE PLANS:
--  Stage: Stage-0
--    Fetch Operator
--      limit: 100
--      Processor Tree:
--        TableScan
--          alias: search_results
--          Statistics: Num rows: 996 Data size: 1101712 Basic stats: COMPLETE Column stats: NONE
--          Select Operator
--            expressions: userid (type: string), unix_time (type: bigint), cheapest_prices_per_hotel_breakfast(map<int:struct<advertisers...) (type: ar
--ray<int>)
--            outputColumnNames: _col0, _col1, _col2
--            Statistics: Num rows: 996 Data size: 1101712 Basic stats: COMPLETE Column stats: NONE
--            Filter Operator
--              predicate: _col2 is not null (type: boolean)
--              Statistics: Num rows: 498 Data size: 550856 Basic stats: COMPLETE Column stats: NONE
--              Select Operator
--                expressions: _col0 (type: string), _col1 (type: bigint), _col2 (type: array<int>)
--                outputColumnNames: _col0, _col1, _col2
--                Statistics: Num rows: 498 Data size: 550856 Basic stats: COMPLETE Column stats: NONE
--                Limit
--                  Number of rows: 100
--                  Statistics: Num rows: 100 Data size: 110600 Basic stats: COMPLETE Column stats: NONE
--                  ListSink
--
--Time taken: 0.085 seconds, Fetched: 27 row(s