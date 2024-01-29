SELECT
	alHead.DocDate AS DocDate,
	PharmacyCode,
	(CASE
		WHEN ((CHARINDEX(';', alHead.RecipeNumber)-1)<> 4) AND ((CHARINDEX(';', alHead.RecipeNumber)-1)<> 6) AND (SUBSTRING(alHead.RecipeNumber, 1, 1)<> '3') THEN 8
		WHEN ((CHARINDEX(';', alHead.RecipeNumber)-1)= 4) OR (((CHARINDEX(';', alHead.RecipeNumber)-1)= 8) AND (SUBSTRING(alHead.RecipeNumber, 1, 1)= '3')) THEN 4
		ELSE (CHARINDEX(';', alHead.RecipeNumber)-1)
	END) AS TypeOrder,
	(CASE  
		WHEN (TRIM(alHead.RecipeNumber)='') THEN 0 
		ELSE 1 
	END) AS OnLineSale ,
	COUNT(DISTINCT(alHead.ID)) AS OrderQuantity,
	FLOOR(SUM(alTabl.DiscountPrice *(alTabl.IntQuantity + alTabl.FracQuantity)-alTabl.DiscByBonusCard)* 100) AS SallingSum,
	FLOOR(SUM(alTabl.PurchasePrice *(alTabl.IntQuantity + IIF(alTabl.FracQuantity = 0, 0, alTabl.FracQuantity / ROUND(alTabl.Price / alTabl.SellingPrice, 0))))* 100) AS PurchaseSum
FROM
	[Central].[dbo].[CheckHeaders] AS alHead
inner join [Central].[dbo].[CheckTables] AS alTabl
  ON
	alHead.ID = alTabl.CheckID
WHERE
	alHead.Status = 1
	AND ISNULL(alHead.WriteOffType, 0)= 0 AND alTabl.DiscountPrice>0
	AND alTabl.DiscountPrice>0 
	AND alTabl.PurchasePrice>0 
	AND alHead.DocDate > '01.01.2022'
	GROUP BY
		alHead.DocDate,
		PharmacyCode,
		(CASE
			WHEN ((CHARINDEX(';', alHead.RecipeNumber)-1)<> 4) AND ((CHARINDEX(';', alHead.RecipeNumber)-1)<> 6) AND (SUBSTRING(alHead.RecipeNumber, 1, 1)<> '3') THEN 8
			WHEN ((CHARINDEX(';', alHead.RecipeNumber)-1)= 4) OR (((CHARINDEX(';', alHead.RecipeNumber)-1)= 8) AND (SUBSTRING(alHead.RecipeNumber, 1, 1)= '3')) THEN 4
			ELSE (CHARINDEX(';', alHead.RecipeNumber)-1)
		END),
		(CASE 
		  WHEN (TRIM(alHead.RecipeNumber)='') THEN 0
		  ELSE 1
		END);
