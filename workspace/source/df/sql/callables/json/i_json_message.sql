{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
/*
    Jose Segarra
    https://www.codeproject.com/Articles/1000953/JSON-for-Sql-Server-Part
*/

IF OBJECT_ID('{{ AF_SDF_ }}json_message', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}json_message
;

CREATE FUNCTION {{ AF_SDF_ }}json_message(@JSON NVARCHAR(MAX),@start int)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    set @start={{ AF_SDF_ }}json_skip(@json,@start)
    if (@start=0) return '** END OF TEXT **'
    return substring(@json,@start,30)
END
;

{% endif %}
