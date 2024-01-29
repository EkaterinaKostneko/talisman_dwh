{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
IF object_id('{{ AF_SDF_ }}try_convert_int', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}try_convert_int;

CREATE FUNCTION {{ AF_SDF_ }}try_convert_int
(
  @value nvarchar(4000)
)
RETURNS bigint
AS
BEGIN
  RETURN (SELECT CONVERT(int, 
                         CASE WHEN LEN(@value) <= 11 THEN
                           CASE WHEN SUBSTRING(@value, 1, 1) LIKE N'[-0-9]' AND 
                                     SUBSTRING(@value, 2, LEN(@value) - 1) NOT LIKE N'%[^0-9]%' THEN
                             CASE WHEN CONVERT(bigint, @value) BETWEEN -2147483648 AND 2147483647 THEN 
                               @value 
                             END 
                           END 
                         END))
END;
{% endif %}
