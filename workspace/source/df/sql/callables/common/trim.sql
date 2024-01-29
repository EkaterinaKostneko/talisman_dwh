{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
IF OBJECT_ID('{{ AF_SDF_ }}trim', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}trim;

CREATE FUNCTION {{ AF_SDF_ }}trim (
  @value nvarchar(max),
  @char_list nvarchar(32)
)
RETURNS nvarchar(max)
AS
BEGIN
  IF @char_list = ' '
    RETURN LTRIM(RTRIM(@value));
    
  RETURN {{ AF_SDF_ }}trim_left({{ AF_SDF_ }}trim_right(@value, @char_list), @char_list)
END;
{% endif %}