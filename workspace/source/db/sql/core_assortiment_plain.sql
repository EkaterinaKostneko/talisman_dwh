TRUNCATE TABLE core.assortiment_plain;

INSERT INTO core.assortiment_plain (
	DocDate,
	IDdoc,
	lineno,
	UpdateDate,
	IDstore,
	IDitem,
	quantity,
	return_quantity,
	move_quantity,
	revenue ,
	purchase,
    discount1 ,
    discount2 ,
    discount3 ,
    discount4 ,
    discount5,
    manufacture,
    kind
)
SELECT
	DocJourn.DocDate    as DocDate,
	TRIM(Doc.iddoc)     as IDdoc,
	DocTabl.lineno_     as lineno,
	Doc.SP21391         as UpdateDate,
	TRIM(Doc.SP21204)   as IDstore,
	TRIM(DocTabl.SP21207) as IDitem,
	DocTabl.SP21209     as quantity,
	DocTabl.SP21210     as return_quantity,
	DocTabl.SP25099     as move_quantity,
	DocTabl.SP21226     as revenue ,
	DocTabl.SP25086     as purchase ,
	DocTabl.SP25076     as discount1 ,
    DocTabl.SP25077     as discount2 ,
    DocTabl.SP25078     as discount3 ,
    DocTabl.SP25079     as discount4 ,
    DocTabl.SP25080     as discount5,
	DocTabl.SP23196     as manufacture,
	Doc.SP21205 as kind
FROM 	stg_dwh.dh21203_retail_reports AS Doc
LEFT JOIN stg_dwh.dt21203_retail_reports  AS DocTabl
	ON Doc.iddoc = DocTabl.iddoc
LEFT JOIN core."_1sjourn" AS DocJourn
	ON Doc.iddoc = DocJourn.iddoc
--WHERE
--    DocTabl.SP25099 = 0 -- убираем возвраты (временно, потом включить в шаги)
--where DocJourn.DocDate between '2023-10-01' and '2023-10-01'