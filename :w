-------------------------------------------------
-- Load the data into Hadoop, and perform a count of the records.
-------------------------------------------------
drop table if exists default.search_results;

create database if not exists default;

create temporary table temp_search_results (
  userid string,
  unix_time bigint,
  hotelresults map<int,struct<advertisers:map<string,array<struct<eurocents:int,breakfast:boolean>>>>>
);

load data inpath '/tmp/data.dat' overwrite into table temp_search_results;

create table default.search_results (
  userid string,
  unix_time bigint,
  hotelresults map<int,struct<advertisers:map<string,array<struct<eurocents:int,breakfast:boolean>>>>>
)
partitioned by (date_of_search string)
stored as ORC;

insert overwrite table default.search_results partition(date_of_search)
select
  *,
  to_date(from_unixtime(unix_time)) as date_of_search
from
  temp_search_results;

drop table temp_rearch_results;

-------
select count(*) from default.search_results;
-------

-------------------------------------------------
-- Find per advertiser and hotel the cheapest price that was offered.
-------------------------------------------------
create or replace view
  default.cheapest_price_by_advertiser_and_hotel
as
with search_results_ranked as (
  select
    advertiser,
    hotelid,
    deal.eurocents,
    row_number() over (partition by advertiser, hotelid order by eurocents) as row_number
  from default.search_results
    lateral view explode(hotelresults) t1 as hotelid, advertisers
    lateral view explode(advertisers.advertisers) t2 as advertiser, deals
    lateral view inline(deals) deal
)
select
  advertiser,
  hotelid,
  eurocents as cheapest_price
from
  search_results_ranked
where
  row_number = 1;

-------
select
  advertiser,
  hotelid,
  cheapest_price
from
  default.cheapest_price_by_advertiser_and_hotel
limit 500;
-------

------------------------------------------------------------------------------------
-- Generate a list containing the cheapest price per hotel that offers breakfast
------------------------------------------------------------------------------------

