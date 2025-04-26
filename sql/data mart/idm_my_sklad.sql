with week_filter as (
SELECT 
    date_trunc('week', date_data) AS week_start,
    date_trunc('week', date_data) + INTERVAL '6 days' AS week_end,
    EXTRACT(WEEK FROM date_trunc('week', date_data))::INTEGER AS week_number,
    "id", 
    "date_data",
    "name",
    "article",
    "sellQuantity", 
    "sellPrice",
    "sellCost",
    "sellSum",
    "sellCostSum",
    "returnQuantity",
    "returnPrice",
    "returnCost",
    "returnSum", 
    "returnCostSum",
    "profit",
    "margin",
    "salesMargin"
FROM profit.profit_by_products
WHERE article != '' 
GROUP BY 
	date_trunc('week', date_data),
	EXTRACT(WEEK FROM date_trunc('week', date_data))::INTEGER,
    "id", 
    "name",
    "article",
    "sellQuantity", 
    "sellPrice",
    "sellCost",
    "sellSum",
    "sellCostSum",
    "returnQuantity",
    "returnPrice",
    "returnCost",
    "returnSum", 
    "returnCostSum",
    "profit",
    "margin",
    "salesMargin"
ORDER BY article , week_start
)
,
week_number_filter as (
SELECT
    "week_number",
    MIN("week_start") AS week_start,
    MIN("week_end") AS week_end,
    "name",
    "article",
    SUM("sellQuantity") AS sellQuantity,
    SUM("sellSum") AS sellSum,
    SUM("sellCostSum") AS sellCostSum,
    SUM("returnQuantity") AS returnQuantity,
    SUM("returnSum") AS returnSum,
    SUM("returnCostSum") AS returnCostSum,
    SUM("profit") AS profit
FROM week_filter
GROUP BY
    "week_number",
    "name",
    "article"
ORDER BY
    "week_number",
    "name",
    "article"
)
select
	week_number,
	week_start,
	week_end,
	name,
	article,
	sellQuantity,
	case
		when sellQuantity = 0 then 0
		else ROUND(sellSum / sellQuantity, 2)
	end as sellPrice,
	case
		when sellQuantity  = 0 then 0
		else ROUND(sellCostSum/sellQuantity, 2)
	end as sellCost,
	sellSum,
	sellCostSum,
	returnQuantity,
	case
		when returnQuantity = 0 then 0
		else ROUND(returnSum/returnQuantity, 2)
	end as returnPrice,
	case
		when returnQuantity  = 0 then 0
		else ROUND(returnCostSum/returnQuantity, 2)
	end as returnCost,
	returnSum,
	returnCostSum,
	case
		when (sellCostSum-returnCostSum) = 0 then 0
		else ROUND((profit/(sellCostSum-returnCostSum)) * 100 , 2)
	end as salesmargin,
	case
		when (sellSum-returnSum) = 0  then 0
		else ROUND((profit/(sellSum-returnSum)) * 100 , 2)
	end as margin,
	profit
from week_number_filter 
