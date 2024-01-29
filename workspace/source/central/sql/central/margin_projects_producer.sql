SELECT
	alHead.DocDate AS DocDate,
	PharmacyCode,
	(CASE
		WHEN ((POSITION(';' IN alHead.RecipeNumber)-1)<> 4) AND ((POSITION(';' IN alHead.RecipeNumber)-1)<> 6) AND (SUBSTRING(alHead.RecipeNumber, 1, 1)<> '3') THEN 8
		WHEN ((POSITION(';' IN alHead.RecipeNumber)-1)= 4) OR (((POSITION(';' IN alHead.RecipeNumber)-1)= 8) AND (SUBSTRING(alHead.RecipeNumber, 1, 1)= '3')) THEN 4
		ELSE (POSITION(';' IN alHead.RecipeNumber)-1)
	END) AS TypeOrder,
	(CASE  
		WHEN (TRIM(alHead.RecipeNumber)='') THEN 0 
		ELSE 1 
	END) AS OnLineSale ,
	COUNT(DISTINCT(alHead.ID)) AS OrderQuantity,
	SUM(totalsum) AS SallingSum,
	SUM(checksum_purchase) AS PurchaseSum
FROM
	stg_dwh.CheckHeaders AS alHead
WHERE
	alHead.Status = 1
	AND alHead.WriteOffType is not null 
	AND alHead.totalsum>0 
	AND alHead.checksum_purchase>0 
	AND alHead.DocDate between '01.09.2023' and '01.10.2023'
	GROUP BY
		alHead.DocDate,
		PharmacyCode,
		(CASE
			WHEN ((POSITION(';' IN alHead.RecipeNumber)-1)<> 4) AND ((POSITION(';' IN alHead.RecipeNumber)-1)<> 6) AND (SUBSTRING(alHead.RecipeNumber, 1, 1)<> '3') THEN 8
			WHEN ((POSITION(';' IN alHead.RecipeNumber)-1)= 4) OR (((POSITION(';' IN alHead.RecipeNumber)-1)= 8) AND (SUBSTRING(alHead.RecipeNumber, 1, 1)= '3')) THEN 4
			ELSE (POSITION(';' IN alHead.RecipeNumber)-1)
		END),
		(CASE 
		  WHEN (TRIM(alHead.RecipeNumber)='') THEN 0
		  ELSE 1
		END);