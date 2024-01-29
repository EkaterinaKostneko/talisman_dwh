TRUNCATE TABLE marts.mart_assortiment_sales_pre;

INSERT INTO marts.mart_assortiment_sales_pre
(
	"Дата",
 	"ID аптеки",
 	"ID товара",
 	"Количество",
	"Сумма",
	"СуммаЗакупа",
	"ID продукта"
	)
SELECT 
	DocJourn.DocDate 	AS "Дата",
	Contractor.ID	 	AS "ID аптеки",
	Product.CODE 		AS "ID товара",
	DocTabl.SP21209 	AS "Количество",
	DocTabl.SP21226 	AS "Сумма",
	DocTabl.SP25086 	AS "СуммаЗакупа",
	DocTabl.SP21207 	AS "ID продукта"
FROM 	stg_dwh.dh21203_retail_reports AS Doc
LEFT JOIN stg_dwh.dt21203_retail_reports  AS DocTabl
	ON Doc.iddoc = DocTabl.iddoc
LEFT JOIN ods."_1sjourn" AS DocJourn 
	ON Doc.iddoc = DocJourn.iddoc
LEFT JOIN  stg_dwh.sc133_contragent AS Contractor
	ON Doc.SP21204 = Contractor.id
LEFT JOIN  stg_dwh.sc156_range AS Product 
	ON DocTabl.SP21207 = Product.id 
WHERE 
DocTabl.SP21226 <>0
