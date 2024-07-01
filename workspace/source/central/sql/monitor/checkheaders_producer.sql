SELECT 
 count(ID) Count,
 CAST([DateTime] AS Date) DT,
 DocDate,
 sum(CheckSum_Purchase) CheckSum_Purchase,
 sum(CheckSum_Selling) CheckSum_Selling,
 sum(CheckSum_WithDiscount) CheckSum_WithDiscount
FROM
	[Central].[dbo].[CheckHeaders]
WHERE
	DocDate > '01.01.2024'
GROUP BY
DocDate,
CAST([DateTime] AS Date)
Order by docdate desc
;