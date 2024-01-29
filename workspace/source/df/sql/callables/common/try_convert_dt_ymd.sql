{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
IF object_id('{{ AF_SDF_ }}try_convert_dt_ymd', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}try_convert_dt_ymd;

CREATE FUNCTION {{ AF_SDF_ }}try_convert_dt_ymd
(
  @value nvarchar(4000)
)
RETURNS date
AS
BEGIN
  DECLARE @date_string nvarchar(10) = REPLACE(REPLACE(SUBSTRING(LTRIM(@value), 1, 10), '-', '.'), '/', '.')
  
  SET @date_string = CASE WHEN @value like '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
                            THEN @value
                          WHEN @date_string like '[0-9][0-9][0-9][0-9].[0-9][0-9].[0-9][0-9]'
                            THEN @date_string
                          WHEN @date_string like '[0-9][0-9].[0-9][0-9].[0-9][0-9][0-9][0-9]'
                            THEN SUBSTRING(@date_string, 7, 4) + '-' + SUBSTRING(@date_string, 4, 2) + '-' + SUBSTRING(@date_string, 1, 2)
                     END
  RETURN (SELECT CONVERT(date, 
                         CASE WHEN ISDATE(@date_string) = 1 THEN @date_string END,
                         102)
         )
END;
{% endif %}
