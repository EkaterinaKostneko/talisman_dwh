{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
/*
    Jose Segarra
    https://www.codeproject.com/Articles/1000953/JSON-for-Sql-Server-Part
*/

IF OBJECT_ID('{{ AF_SDF_ }}json_number', 'TF') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}json_number
;

CREATE FUNCTION {{ AF_SDF_ }}json_number(@JSON NVARCHAR(MAX),@start int)
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
    declare @cof   nvarchar(max)=''
    declare @v nvarchar(max)=''

    insert @data(p1,p2,value) select 1,@start+1,100

    -- Parse NEGATIVE sign
    if (SUBSTRING(@json,@start,1)='-') 
    begin
        set @value='-'
        set @start=@start+1
    end 

    -- Parse integer part of number
    set @v={{ AF_SDF_ }}json_natural(@JSON,@start)
    set @value=@value+@v
    set @start=@start+len(@v)
    
    -- Let's handle .
    if (SUBSTRING(@json,@start,1)='.') 
    begin
        set @value=@value+'.'
        set @start=@start+1
        set @v={{ AF_SDF_ }}json_natural(@JSON,@start)
        if (@v='') 
        begin 
            insert @data(p1,p2,value) select @p1,-1,'Expected fractional part when parsing Number'
            return
        end
        set @value=@value+@v
        set @start=@start+len(@v)
    end
    -- If this is an EXPO
    if (lower(SUBSTRING(@json,@start,1))='e') 
    begin
        set @start=@start+1
        set @cof=SUBSTRING(@json,@start,1)
        if (@cof!='+') and (@cof!='-') and (@cof<'0') AND (@cof > '9') 
        begin
            insert @data(p1,p2,value) select @p1,-1,'Expected sign in coeficient part when parsing Number'
            return
        end
        if (@cof='+') or (@cof='-') set @start=@start+1 else set @cof='+'
        set @v={{ AF_SDF_ }}json_natural(@JSON,@start)
        if (@v='') 
        begin 
            insert @data(p1,p2,value) select @p1,-1,'Expected coeficient part when parsing Number'
            return
        end
        set @start=@start+len(@v)
        -- Make a numeric value
        set @value=convert(nvarchar(max),convert(float,@value+'E'+@cof+@v))
    end
    
    -- Insert value
    insert @data(p1,p2,value) select @p1,@start,@value
    return
END
;

{% endif %}
