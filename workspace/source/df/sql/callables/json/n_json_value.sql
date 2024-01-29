{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
/*
    Jose Segarra
    https://www.codeproject.com/Articles/1000953/JSON-for-Sql-Server-Part
*/

IF OBJECT_ID('{{ AF_SDF_ }}json_value', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}json_value
;

CREATE FUNCTION {{ AF_SDF_ }}json_value(@json nvarchar(max),@path nvarchar(max))
RETURNS nvarchar(MAX) 
AS
BEGIN
    declare @data json_data
    declare @p1   TABLE(id int identity(1,1),name nvarchar(max))
    
    insert into @data       select id,parent,name,kind,value from {{ AF_SDF_ }}json_parse(@json)
    set @path=ltrim(rtrim(@path))
    if @path!='' insert into @p1(name)   select * from {{ AF_SDF_ }}split_string(@path,'.')
    
    declare @c0    int =0
    declare @cur    int =null
    declare @step   int =1
    declare @max    int =1
    declare @v      nvarchar(max)
    declare @v2      nvarchar(max)
    declare @kind   nvarchar(max)
    select  @max=max(id) from @p1
    select @c0=id,@cur=id, @kind=kind,@v=value from @data where parent=0           -- Current object is the one with parent =0
    if @cur is null return null                                             -- Should not happen
    while (@step<=@max and @c0 is not null) 
    begin
        select @v=name from @p1 where id=@step
        set @c0 =null
        if (IsNumeric(@v)=1) 
        begin
            if (@kind!='ARRAY') return cast('Using index in non array JSON' as int);
            set @v2=@v
            --SELECT @c0=ID,@cur=ID,@kind=kind,@v=value FROM @data where parent=@cur ORDER BY ID OFFSET convert(int,@v) ROWS FETCH FIRST 1 ROW ONLY
            SELECT @c0=ID,@cur=ID,@kind=kind,@v=value FROM (
                SELECT *, ROW_NUMBER() over (order by ID) as rnum FROM @data where parent=@cur
            ) t
            WHERE rnum = convert(int,@v)
        end 
        else 
        begin
            if (@kind!='OBJECT') return cast('Using property name in a non-object JSON' as int);
            SELECT @c0=ID,@cur=ID,@kind=kind,@v=value FROM @data where parent=@cur and name=@v
        end
        set @step=@step+1
    end
    if @c0 is null return null
    if (@kind='OBJECT' or @kind='ARRAY') return {{ AF_SDF_ }}json_to_json(@data,@cur)
    return @v
END
;

{% endif %}
