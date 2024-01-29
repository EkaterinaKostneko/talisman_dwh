{#
  Приведение строки к дате с проверкой на возможность приведения (установленный в СУБД формат)
#}
{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
IF object_id('{{ AF_SDF_ }}try_convert_dt', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}try_convert_dt;

CREATE FUNCTION {{ AF_SDF_ }}try_convert_dt
(
  @value nvarchar(4000)
)
RETURNS date
AS
BEGIN
  RETURN (SELECT CONVERT(date, 
                         CASE WHEN ISDATE(@value) = 1 THEN @value END)
         )
END;
{% endif %}
