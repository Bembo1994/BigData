CREATE TABLE IF NOT EXISTS stockprices (
 ticker string,
 open float,
 close float,
 adj_close float,
 low float,
 high float,
 volume int,
 data date)
 COMMENT 'Historical Stock Prices'
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ','
 TBLPROPERTIES("skip.header.line.count"="1");
 
LOAD DATA LOCAL INPATH '/home/hadoop/historical_stock_prices.csv' OVERWRITE INTO TABLE stockprices;

set SOGLIA=1;
set hive.mapred.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

create view if not exists main2017 as (
	select stockprices.ticker as ticker, stockprices.data as data, stockprices.close as close
	from stockprices
	where year(stockprices.data) = 2017
);

create view if not exists app_date as (
	select month(data) as mese, min(data) as min_data, max(data) as max_data from main2017 group by month(data)
);

create view if not exists var_moth_ticker as (
	select x.ticker, x.mese, ((y.xf-x.xi)/x.xi) * 100 as variazione
	from (
		select main2017.ticker as ticker ,month(main2017.data) as mese, main2017.close as xi from main2017, app_date
		where main2017.data = app_date.min_data group by month(main2017.data), main2017.ticker, main2017.close
	) as x
	join (
		select main2017.ticker as ticker ,month(main2017.data) as mese, main2017.close as xf from main2017, app_date
		where main2017.data = app_date.max_data group by month(main2017.data), main2017.ticker, main2017.close
	) as y 
	on x.ticker = y.ticker 
	where x.mese = y.mese 
	order by ticker, mese
);

create view if not exists app_result as (
	select ticker, collect_set(variazione) as mesi
	from var_moth_ticker
	group by ticker
);

select a.ticker, b.ticker, cast(a.mesi[0] as decimal(15,3)), cast(b.mesi[0] as decimal(15,3)), cast(a.mesi[1] as decimal(15,3)), cast(b.mesi[1] as decimal(15,3)), cast(a.mesi[2] as decimal(15,3)), cast(b.mesi[2] as decimal(15,3)), cast(a.mesi[3] as decimal(15,3)), cast(b.mesi[3] as decimal(15,3)), cast(a.mesi[4] as decimal(15,3)), cast(b.mesi[4] as decimal(15,3)),cast(a.mesi[5] as decimal(15,3)), cast(b.mesi[5] as decimal(15,3)), cast(a.mesi[6] as decimal(15,3)), cast(b.mesi[6] as decimal(15,3)), cast(a.mesi[7] as decimal(15,3)), cast(b.mesi[7] as decimal(15,3)), cast(a.mesi[8] as decimal(15,3)), cast(b.mesi[8] as decimal(15,3)), cast(a.mesi[9] as decimal(15,3)), cast(b.mesi[9] as decimal(15,3)), cast(a.mesi[10] as decimal(15,3)), cast(b.mesi[10] as decimal(15,3)), cast(a.mesi[11] as decimal(15,3)), cast(b.mesi[11] as decimal(15,3))
from app_result as a join app_result as b on a.ticker != b.ticker
where (abs(a.mesi[0] - b.mesi[0])) <= ${hiveconf:SOGLIA} and (abs(a.mesi[1] - b.mesi[1])) <= ${hiveconf:SOGLIA} and 
(abs(a.mesi[2] - b.mesi[2])) <= ${hiveconf:SOGLIA} and (abs(a.mesi[3] - b.mesi[3])) <= ${hiveconf:SOGLIA} and 
(abs(a.mesi[4] - b.mesi[4])) <= ${hiveconf:SOGLIA} and (abs(a.mesi[5] - b.mesi[5])) <= ${hiveconf:SOGLIA} and 
(abs(a.mesi[6] - b.mesi[6])) <= ${hiveconf:SOGLIA} and (abs(a.mesi[7] - b.mesi[7])) <= ${hiveconf:SOGLIA} and 
(abs(a.mesi[8] - b.mesi[8])) <= ${hiveconf:SOGLIA} and (abs(a.mesi[9] - b.mesi[9])) <= ${hiveconf:SOGLIA} and
(abs(a.mesi[10] - b.mesi[10])) <= ${hiveconf:SOGLIA} and (abs(a.mesi[11] - b.mesi[11])) <= ${hiveconf:SOGLIA};


