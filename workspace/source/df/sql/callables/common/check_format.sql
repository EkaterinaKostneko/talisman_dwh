{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
IF object_id('{{ AF_SDF_ }}check_format', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}check_format;

CREATE FUNCTION {{ AF_SDF_ }}check_format
(
  @value nvarchar(4000),
  @format nvarchar(1024)
)
RETURNS bit
AS
BEGIN
  RETURN (cast(patindex(@format, @value) as bit))
END;
{% endif %}