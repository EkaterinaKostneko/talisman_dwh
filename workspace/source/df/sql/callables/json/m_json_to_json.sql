{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
/*
    Jose Segarra
    https://www.codeproject.com/Articles/1000953/JSON-for-Sql-Server-Part
*/

IF OBJECT_ID('{{ AF_SDF_ }}json_to_json', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}json_to_json
;


CREATE  FUNCTION {{ AF_SDF_ }}json_to_json(@data json_data READONLY,@node int)
returns nvarchar(max)
AS 
BEGIN
    declare @kind   nvarchar(max)
    declare @value  nvarchar(max)

    select @kind=kind,@value=value from @data where id=@node
    if (@kind='STRING') return '"'+@value+'"'
    if (@kind='NUMBER') return @value
    if (@kind='BOOL') 
    begin
        if @value=1 return 'True' else return 'False'
    end
    if (@kind='OBJECT') 
    begin
        set @value=''
        SELECT @value= @value+ ',"'+Name +'":'+{{ AF_SDF_ }}json_to_json(@data,id) FROM @data where parent=@node
        return '{'+case when @value='' then '' else substring(@value,2,len(@value)-1) end+'}'
    end
    if (@kind='ARRAY') 
    begin
        set @value=''
        SELECT @value= @value+ ','+{{ AF_SDF_ }}json_to_json(@data,id) FROM @data where parent=@node
        return '['+case when @value='' then '' else substring(@value,2,len(@value)-1) end+']'
    end
    return cast('Unkown KIND in Json Data' as int);
    return '*ERROR*'
END
;

{% endif %}
