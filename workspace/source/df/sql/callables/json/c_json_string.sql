{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
/*
    Jose Segarra
    https://www.codeproject.com/Articles/1000953/JSON-for-Sql-Server-Part
*/

IF OBJECT_ID('{{ AF_SDF_ }}json_string', 'TF') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}json_string
;

CREATE FUNCTION {{ AF_SDF_ }}json_string(@JSON NVARCHAR(MAX),@start int)
RETURNS @data table
(
    p1 int,
    p2   int,
    value nvarchar(max)
)
AS 
BEGIN
    declare @p1 int =@start
    declare @value nvarchar(max)=''
    while (@start<=len(@json) and SUBSTRING(@json,@start,1)!='"') 
    begin
        if (SUBSTRING(@json,@start,1)='\') set @start=@start+1
        if @start<len(@json) set @value=@value+SUBSTRING(@json,@start,1)
        set @start=@start+1
    end
    if (@start>len(@json)) 
    begin
        insert @data(p1,p2,value) select @p1,-1,'Unterminated string'
        return
    end 
    insert @data(p1,p2,value) select @p1,@start+1,@value 
    return
END
;

{% endif %}
