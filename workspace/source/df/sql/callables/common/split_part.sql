{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
IF OBJECT_ID('{{ AF_SDF_ }}split_part', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}split_part;

CREATE FUNCTION {{ AF_SDF_ }}split_part
(
  @string nvarchar(max),
  @delimiter nvarchar(255),
  @num int
)
RETURNS nvarchar(4000)
AS
BEGIN
  RETURN (SELECT TOP 1 item from {{ AF_SDF_ }}split_part_tab(@string, @delimiter, @num));
END;
{% endif %}