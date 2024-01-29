{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
/*
    Jose Segarra
    https://www.codeproject.com/Articles/1000953/JSON-for-Sql-Server-Part
*/

IF OBJECT_ID('{{ AF_SDF_ }}json_skip', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}json_skip
;

CREATE  FUNCTION {{ AF_SDF_ }}json_skip(@JSON NVARCHAR(MAX),@start int)
RETURNS int
AS
BEGIN
    if (@start>=len(@json)) return 0
    while ((@start<=len(@json)) and (ascii(substring(@json,@start,1))<=32)) set @start=@start+1
    if (@start>len(@json)) return 0
    return @start
END
;

{% endif %}
