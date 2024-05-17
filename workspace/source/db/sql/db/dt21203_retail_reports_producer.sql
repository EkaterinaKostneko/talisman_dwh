select
	DocTabl.iddoc,
    lineno_,
    SP21207,
    SP21209,
    SP21210 ,
    SP25099 ,
    SP21211 ,
    SP21451 ,
    SP21226 ,
    SP23196 ,
    SP25076 ,
    SP25077 ,
    SP25078 ,
    SP25079 ,
    SP25080 ,
    SP25081 ,
    SP25082 ,
    SP25083 ,
    SP25084 ,
    SP25085 ,
    SP25086 
from
	dt21203 DocTabl
--where SP21391 between '2023-10-01' and '2023-10-31'
left join _1SJOURN
on DocTabl.iddoc = _1SJOURN.iddoc
where CAST (SUBSTRING(_1SJOURN.DATE_TIME_IDDOC, 1, 8) as date) between '2023-10-01' and '2024-10-31';