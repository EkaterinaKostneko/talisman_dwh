{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
/*
    Jose Segarra
    https://www.codeproject.com/Articles/1000953/JSON-for-Sql-Server-Part
*/

IF OBJECT_ID('{{ AF_SDF_ }}json_skip_until', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}json_skip_until
;

CREATE FUNCTION {{ AF_SDF_ }}json_skip_until(@JSON NVARCHAR(MAX),@start int,@what NVARCHAR(MAX))
RETURNS int
AS
BEGIN
    while ((@start<=len(@json)) and (ascii(substring(@json,@start,1))<=32)) 
    begin
        set @start=@start+1
    end
    if (@start>len(@json)) return 0
    if (substring(@json,@start,len(@what))!=@what) return 0
    return @start+1
END
;

{% endif %}
