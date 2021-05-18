CREATE TABLE IF NOT EXISTS stockprices (
 ticker string,
 open string,
 close string,
 adj_close string,
 low string,
 high string,
 volume string,
 data string)
 COMMENT 'Historical Stock Prices'
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ','
 TBLPROPERTIES("skip.header.line.count"="1");
  
LOAD DATA LOCAL INPATH '/home/hadoop/historical_stock_prices.csv' OVERWRITE INTO TABLE stockprices;


create view if not exists appoggio as (select ticker as ticker, min(data) as min_data, max(data) as max_data from stockprices group by ticker);

create view if not exists ticker_close_min as (select stockprices.ticker,close from stockprices join appoggio on stockprices.ticker = appoggio.ticker where stockprices.data = appoggio.min_data);

create view if not exists ticker_close_max as (select stockprices.ticker,close from stockprices join appoggio on stockprices.ticker = appoggio.ticker where stockprices.data = appoggio.max_data);

create view if not exists ticker_variation as (select ticker_close_max.ticker, (ticker_close_max.close-ticker_close_min.close)/ticker_close_min.close as variation from ticker_close_max join ticker_close_min on ticker_close_max.ticker = ticker_close_min.ticker);
                                    
SELECT stockprices.ticker, min(stockprices.data) AS first_data, MAX(stockprices.data) AS last_data, cast(ticker_variation.variation*100  as decimal(15,3)) AS variation_percentage, cast(MIN(stockprices.low)as decimal(15,3)) AS min_price, cast(MAX(stockprices.high) as decimal(15,3)) AS max_price
FROM stockprices join ticker_variation on stockprices.ticker = ticker_variation.ticker
GROUP BY stockprices.ticker, ticker_variation.variation
ORDER BY last_data DESC;



