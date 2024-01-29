{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
/*
    Jose Segarra
    https://www.codeproject.com/Articles/1000953/JSON-for-Sql-Server-Part
*/

IF OBJECT_ID('{{ AF_SDF_ }}json_natural', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}json_natural
;

CREATE FUNCTION {{ AF_SDF_ }}json_natural(@JSON NVARCHAR(MAX),@start int)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    declare @value NVARCHAR(MAX)=''
    while SUBSTRING(@json,@start,1) >= '0' AND SUBSTRING(@json,@start,1) <= '9' 
    begin
        set @value=@value +SUBSTRING(@json,@start,1)
        set @start=@start+1
    end
    return @value
END
;

{% endif %}
