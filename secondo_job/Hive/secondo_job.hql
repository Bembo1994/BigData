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
 
CREATE TABLE IF NOT EXISTS stock (
 id int,
 ticker string,
 scambio string,
 name string,
 sector string,
 industry string)
 COMMENT 'Historical Stock'
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ','
 TBLPROPERTIES("skip.header.line.count"="1");
 
LOAD DATA LOCAL INPATH '/home/hadoop/historical_stock_prices.csv' OVERWRITE INTO TABLE stockprices;
LOAD DATA LOCAL INPATH '/home/hadoop/clean_hs.csv' OVERWRITE INTO TABLE stock;

create view if not exists main as (
	select stockprices.ticker, stockprices.data, stockprices.close, stockprices.volume, stock.sector
	from stockprices join stock on stockprices.ticker = stock.ticker
	where year(stockprices.data) >= 2009 and year(stockprices.data) <= 2018 and stock.sector != "nan"
);

create view if not exists sum_vol as (
	select a.sector as sector, a.yyyy as anno, b.ticker as ticker, a.sv as max_vol 
	from (
		select app.sector, app.yyyy, max(app.sum_vol) as sv 
		from ( select sector as sector ,ticker as ticker ,year(data) as yyyy, sum(volume) as sum_vol from main group by sector,
		ticker, year(data) ) as app group by sector, yyyy 
	) as a 
	join (
		select sector as sector ,ticker as ticker ,year(data) as yyyy, sum(volume) as sum_vol from main group by sector, ticker, 
		year(data) 
	) as b 
	on a.sector = b.sector 
	where a.sv = sum_vol 
);

create view if not exists appoggio_date as (
	select year(data) as anno, min(data) as min_data, max(data) as max_data from main group by year(data)
);

create view if not exists var_sector as (
	select x.sector as sector, x.anno as anno, ((y.xf-x.xi)/x.xi) * 100 as variazione
	from (
		select main.sector as sector ,year(main.data) as anno, sum(main.close) as xi from main, appoggio_date where main.data = 			appoggio_date.min_data group by main.sector,year(main.data) 
	) as x 
	join (
		select main.sector as sector ,year(main.data) as anno, sum(main.close) as xf from main, appoggio_date where main.data =
		appoggio_date.max_data group by main.sector,year(main.data) 
	) as y 
	on x.sector = y.sector and x.anno = y.anno

);


create view if not exists var_ticker as (
	select x.sector, x.ticker, x.anno, max(((y.xf-x.xi)/x.xi) * 100) as variazione
	from (
		select main.sector as sector,main.ticker as ticker ,year(main.data) as anno, main.close as xi from main, appoggio_date
		where main.data = appoggio_date.min_data group by main.sector, year(main.data), main.ticker, main.close
	) as x
	join (
		select main.sector as sector,main.ticker as ticker ,year(main.data) as anno, main.close as xf from main, appoggio_date
		where main.data = appoggio_date.max_data group by main.sector, year(main.data), main.ticker, main.close
	) as y 
	on x.sector = y.sector and x.anno = y.anno and x.ticker = y.ticker
	group by x.sector, x.ticker, x.anno
);

create view if not exists max_var_ticker as (
	select x.sector as sector, x.anno as anno, y.ticker as ticker, x.max_var as max_var
	from (
		select var_ticker.sector, var_ticker.anno, max(var_ticker.variazione) as max_var
		from var_ticker
		group by  var_ticker.sector, var_ticker.anno
	) as x
	join var_ticker as y on x.sector = y.sector and x.anno = y.anno and x.max_var = y.variazione


);

select v1.sector, v1.anno, cast(v1.variazione as decimal(15,3)), v2.ticker, cast(v2.max_var as decimal(15,3)), v3.ticker, v3.max_vol
from var_sector as v1 join max_var_ticker as v2 on v1.sector = v2.sector join sum_vol as v3 on v2.sector = v3.sector
where v1.anno = v2.anno and v1.anno = v3.anno
order by sector, anno;

