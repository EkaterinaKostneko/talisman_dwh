{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
/*
    Jose Segarra
    https://www.codeproject.com/Articles/1000953/JSON-for-Sql-Server-Part
*/

IF OBJECT_ID('{{ AF_SDF_ }}json_name', 'TF') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}json_name
;

CREATE FUNCTION {{ AF_SDF_ }}json_name(@start int,@JSON NVARCHAR(MAX))
RETURNS @hierarchy table
(
  kind nvarchar(5),name nvarchar(max),pend int
)
AS
BEGIN
    declare @p1 int
    declare @name nvarchar(max)
    
    set @p1 = {{ AF_SDF_ }}json_skip_until(@json,@start,'"')
    if (@p1=0) 
    begin
        insert into @hierarchy(kind,name,pend) values('ERROR','Expected ["] in property name and have: '+substring(@json,@start,10),@start)
        return;
    end
    select @name=value,@start=p2 from {{ AF_SDF_ }}json_string(@json,@p1)
    if (@start=-1) 
    begin
        insert into @hierarchy(kind,name,pend) values('ERROR',@name,@p1)
        return;
    end
    set @p1 = {{ AF_SDF_ }}json_skip_until(@json,@start,':')
    if (@p1=0) 
    begin
        insert into @hierarchy(kind,name,pend) values('ERROR','Expected [:] after name and have: '+substring(@json,@start,10),@start)
        return;
    end
    
    set @start = {{ AF_SDF_ }}json_skip(@json,@p1)
    if (@start=0) 
    begin
        insert into @hierarchy(kind,name,pend) values('ERROR','Expected a value after [:] and have: '++substring(@json,@p1,10),@p1)
        return;
    end
    insert into @hierarchy(kind,name,pend) values('OK',@name,@start)
    return
END
;

{% endif %}
