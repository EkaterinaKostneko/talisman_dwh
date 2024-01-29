{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
IF object_id('{{ AF_SDF_ }}try_convert_float', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}try_convert_float;

CREATE FUNCTION {{ AF_SDF_ }}try_convert_float
(
  @value nvarchar(4000)
)
RETURNS float
AS
BEGIN
  DECLARE @tmp table (col varchar(4000));
  INSERT INTO @tmp VALUES (@value);
  RETURN (SELECT cast('' as xml).value('sql:column("col") cast as xs:float ?', 
                                       'float') 
            FROM @tmp)
END;
{% endif %}