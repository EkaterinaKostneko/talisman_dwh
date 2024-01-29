{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
IF object_id('{{ AF_SDF_ }}try_convert_dtm', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}try_convert_dtm;

CREATE FUNCTION {{ AF_SDF_ }}try_convert_dtm
(
  @value nvarchar(4000)
)
RETURNS datetime
AS
BEGIN
  RETURN (SELECT CAST(CASE WHEN ISDATE(@value) = 1 THEN @value END AS datetime))
END;
{% endif %}