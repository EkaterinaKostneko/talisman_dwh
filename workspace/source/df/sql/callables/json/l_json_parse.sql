{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
/*
    Jose Segarra
    https://www.codeproject.com/Articles/1000953/JSON-for-Sql-Server-Part
*/

IF OBJECT_ID('{{ AF_SDF_ }}json_parse', 'TF') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}json_parse
;

CREATE FUNCTION {{ AF_SDF_ }}json_parse(@json nvarchar(max))
RETURNS @data TABLE
(
    id int NOT NULL,
    parent int NOT NULL,
    name nvarchar(100) NOT NULL,
    kind nvarchar(10) NOT NULL,
    value nvarchar(MAX) NOT NULL
)
AS
BEGIN
    declare @start int = 1
    set @start = {{ AF_SDF_ }}json_skip(@json,@start)
    
    insert into @data(id,parent,name,kind,value)
    select id,parent,name,kind,value from {{ AF_SDF_ }}json_item(1,0,@start,@json,0)
    return
END
;

{% endif %}
