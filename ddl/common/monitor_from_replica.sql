SELECT top 1
'checkheaders' 	"table", docdate "date"
FROM
dbo.CheckHeaders
order by docDate desc


SELECT top 1
'checktables'	"table", docdate "date"
from dbo.CheckTables
order by docdate desc