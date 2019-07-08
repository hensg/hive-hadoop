-------------------------------------------------
-- Load the data into Hadoop, and perform a count of the records.
-------------------------------------------------
drop table if exists default.search_results;

create database if not exists default;

create temporary table default.tmp_search_results (
  userid string,
  unix_time bigint,
  hotelresults map<int,struct<advertisers:map<string,array<struct<eurocents:int,breakfast:boolean>>>>>
);

load data inpath '/tmp/search_results.dat' into table default.tmp_search_results;

-------------------
-- count raw data
select count(*) from default.tmp_search_results;
-------------------


create table default.search_results (
  userid string,
  unix_time bigint,
  hotelresults map<int,struct<advertisers:map<string,array<struct<eurocents:int,breakfast:boolean>>>>>
)
partitioned by (date_of_search string);

insert overwrite table default.search_results partition(date_of_search)
select
  *,
  to_date(from_unixtime(unix_time)) as date_of_search
from
  tmp_search_results
where
  userid is not null and unix_time is not null;

drop table default.tmp_search_results;

-------------------
-- count good data
select count(*) from default.search_results;
-------------------

-------------------------------------------------
-- Find per advertiser and hotel the cheapest price that was offered.
-------------------------------------------------
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



------------------------------------------------------------------------------------
-- For each search generate a list containing the cheapest price per hotel that offers breakfast
------------------------------------------------------------------------------------
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
