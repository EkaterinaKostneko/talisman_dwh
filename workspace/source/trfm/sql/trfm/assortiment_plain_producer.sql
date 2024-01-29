SELECT
	DocJourn.DocDate as DocDate,
	Doc.iddoc as IDdoc,
	DocTabl.lineno_ as lineno,
	Doc.SP21391 as UpdateDate,
	Doc.SP21204 as IDstore,
	DocTabl.SP21207 as IDitem,
	DocTabl.SP21209 as quantity,
	DocTabl.SP21210 as return_quantity,
	DocTabl.SP25099 as move_quantity,
	DocTabl.SP21226 as revenue ,
	DocTabl.SP25086 as purchase ,
	DocTabl.SP25076 as discount1 ,
    DocTabl.SP25077 as discount2 ,
    DocTabl.SP25078 as discount3 ,
    DocTabl.SP25079 as discount4 ,
    DocTabl.SP25080 as discount5,
	DocTabl.SP23196 as manufacture
FROM 	stg_dwh.dh21203_retail_reports AS Doc
LEFT JOIN stg_dwh.dt21203_retail_reports  AS DocTabl
	ON Doc.iddoc = DocTabl.iddoc
LEFT JOIN core."_1sjourn" AS DocJourn
	ON Doc.iddoc = DocJourn.iddoc