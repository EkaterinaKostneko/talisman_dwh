{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
IF OBJECT_ID('{{ AF_SDF_ }}trim_right', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}trim_right;

CREATE FUNCTION {{ AF_SDF_ }}trim_right
(
  @value nvarchar(max),
  @char_list nvarchar(32)
)
RETURNS nvarchar(max)
AS
BEGIN
   IF @char_list = ' '
     RETURN rtrim(@value);
     
  DECLARE @result nvarchar(max)
  SET @value = '@' + @value
  SET @result = SUBSTRING(REVERSE(@value),
                                  PATINDEX('%[^' + @char_list + ' ]%', REVERSE(@value)),
                                  LEN(@value))
  RETURN REVERSE(SUBSTRING(@result, 1, LEN(@result) - 1))
END;
{% endif %}