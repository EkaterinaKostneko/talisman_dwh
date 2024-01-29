SELECT 
	alTabl.ID AS ID,
	alHead.ID AS CheckID,
	alHead.DocDate AS DocDate,
	PharmacyCode,
	alHead.RecipeNumber,
	alTabl.PurchasePrice,
	alTabl.IntQuantity,
	alTabl.FracQuantity,
	alTabl.Price,
	alTabl.SellingPrice,
	alTabl.DiscountPrice,
	alTabl.DiscByBonusCard,
	alHead.Status,
	alHead.WriteOffType
FROM 
	[Central].[dbo].[CheckHeaders] AS alHead
INNER JOIN [Central].[dbo].[CheckTables] AS alTabl
  ON
	alHead.ID = alTabl.CheckID
WHERE 	
--	alHead.DocDate BETWEEN '01.01.2022' AND '30.09.2023'
	alHead.DocDate >= '01.01.2023'
	AND alHead.Status = 1
	AND ISNULL(alHead.WriteOffType,0)= 0
	AND alTabl.DiscountPrice>0
;