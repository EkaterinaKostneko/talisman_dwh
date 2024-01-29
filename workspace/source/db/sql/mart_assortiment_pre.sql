TRUNCATE TABLE marts.mart_assortiment_sales_pre;

INSERT INTO marts.mart_assortiment_sales_pre
(
	"Дата",
 	"ID аптеки",
 	"ID товара",
 	"Количество",
	"Сумма",
	"СуммаСкидок",
	"СуммаЗакупа",
	"ID продукта"
	)
SELECT 
	DocJourn.DocDate 	AS "Дата",
	TRIM(Contractor.ID)	 	AS "ID аптеки",
	TRIM(Product.CODE) 		AS "ID товара",
	SUM(DocTabl.SP21209) 	AS "Количество",
	SUM(DocTabl.SP21226) 	AS "Сумма",
	SUM(DocTabl.SP25086) 	AS "СуммаЗакупа",
		SUM(DocTabl.sp25076)+
		SUM(DocTabl.sp25077)+
		SUM(DocTabl.sp25078)+
		SUM(DocTabl.sp25079)+
		SUM(DocTabl.sp25080)
 	AS "СуммаСкидок",
	TRIM(DocTabl.SP21207) 	AS "ID продукта"
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
and Doc.sp21205 = 1
	group by
	DocJourn.DocDate,
	TRIM(Contractor.ID),
	TRIM(Product.CODE),
	TRIM(DocTabl.SP21207)