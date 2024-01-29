SELECT
iddoc,
lineno_,
sp22329,
sp24982,
sp22426,
sp22866,
sp22867,
sp23176,
sp23502,
sp23940,
sp23941,
sp23942,
sp24200,
sp25540,
sp25541,
sp25542,
sp26457,
sp26458,
sp26459,
sp26460,
sp26461,
sp26462,
sp26463
FROM dt22325
--left join _1SJOURN  
--on DocTabl.iddoc = _1SJOURN.iddoc 
--where CAST (SUBSTRING(_1SJOURN.DATE_TIME_IDDOC, 1, 8) as date) between cast('{{ AF_INC_BEGIN }}' as DATETIME2) and cast('{{AF_INC_END }}' as DATETIME2)
;

