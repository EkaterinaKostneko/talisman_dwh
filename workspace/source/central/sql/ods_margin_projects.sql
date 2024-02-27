TRUNCATE TABLE ods.margin_projects;

INSERT INTO ods.margin_projects
(
	DocDate,
	PharmacyCode,
	TypeOrder,
	OnlineSale,
	OrderQuantity,
	SallingSum,
	PurchaseSum)
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
	SUM(totalsum)-SUM(DiscountSum) AS SallingSum,
	SUM(checksum_purchase) AS PurchaseSum
FROM
	ods.CheckHeaders AS alHead
WHERE
	alHead.Status = 1
	AND alHead.WriteOffType is not null 
	AND alHead.totalsum>0 
	AND alHead.checksum_purchase>0 
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