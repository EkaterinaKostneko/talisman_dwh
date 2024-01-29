{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
IF OBJECT_ID('{{ AF_SDF_ }}trim_left', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}trim_left;

CREATE FUNCTION {{ AF_SDF_ }}trim_left
(
  @value nvarchar(max),
  @char_list nvarchar(32)
)
RETURNS nvarchar(max)
AS
BEGIN
   IF @char_list = ' '
     RETURN ltrim(@value);
     
   DECLARE @result nvarchar(max)
   SET @value = @value + '@' 
   SET @result = SUBSTRING(@value,
                           PATINDEX('%[^' + @char_list + ' ]%', @value),
                           LEN(@value))
   RETURN SUBSTRING(@result, 1, LEN(@result)-1)
END;
{% endif %}