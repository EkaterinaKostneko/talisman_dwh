{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
/*
    Jose Segarra
    https://www.codeproject.com/Articles/1000953/JSON-for-Sql-Server-Part
*/

IF OBJECT_ID('{{ AF_SDF_ }}json_object', 'TF') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}json_object
;

-- When entering here we know for sure that we are after '{'
CREATE FUNCTION {{ AF_SDF_ }}json_object(@id int,@parent int, @start int,@JSON NVARCHAR(MAX))
RETURNS @hierarchy table
(
  id int,parent int,name nvarchar(2000),kind nvarchar(10),ppos int,pend int,value nvarchar(MAX) NOT NULL
)
AS
BEGIN
    declare @pos int,@m int,@b int=@start
    -- Look for first item after {
    set @pos={{ AF_SDF_ }}json_skip(@json,@start)

    -- IF arrived to the end
    if (@pos = 0) 
    begin
        --insert into @hierarchy(id,parent,name,kind,ppos,pend,value) values(@id,@parent,'','JAR',@start,-1,'['+right(@json,1)+']')
        if (right(@json,1)='}') return -- If found because this is an empty object then do not raise an error
        insert into @hierarchy(id,parent,name,kind,ppos,pend,value) values(@id,@parent,'','ERROR',@start,-1,'Wrong object definition')
        return
    end
    -- If is } then we have anempty object, lets return
    if (substring(@json,@pos,1)='}') return

    -- Enter endless loop
    while (1=1) 
    begin
        -- Insert item into hierarchy
        insert into @hierarchy(id,parent,name,kind,ppos,pend,value) select id,parent,name,kind,ppos,pend,value from {{ AF_SDF_ }}json_item(@id,@parent,@pos,@json,1)
        -- If nothing was inserted then return    
        if not exists(select * from @hierarchy) 
        begin
            insert into @hierarchy(id,parent,name,kind,ppos,pend,value) values(0,0,'','ERROR',@pos,-1,'Unexpected error')
            break
        end
        -- If an error happened then return
        if exists(select * from @hierarchy where kind='ERROR') break
        -- Get MAX id of inserted objects and ADD 1. This sets the new ID
        select @id=max(id)+1 from @hierarchy 
        -- Get latest position of readed object
        select @m=max(pend) from @hierarchy 
        -- Skip after 

        set @pos = {{ AF_SDF_ }}json_skip(@json,@m)
        -- If we do not have a [,] then exit loop
        if (substring(@json,@pos,1)!=',') 
            break
        -- Move after ,
        set @pos = {{ AF_SDF_ }}json_skip(@json,@pos+1)
    end
    return
END
;

{% endif %}
