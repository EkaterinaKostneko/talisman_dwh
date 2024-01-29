{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
IF OBJECT_ID('{{ AF_SDF_ }}split_string_ord', 'TF') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}split_string_ord;

CREATE FUNCTION {{ AF_SDF_ }}split_string_ord
(
  @string NVARCHAR(MAX),
  @delimiter NVARCHAR(255) = ','
)
RETURNS @items TABLE (ord int, item NVARCHAR(4000))
WITH SCHEMABINDING
AS
BEGIN
  DECLARE @ll INT = LEN(@string) + 1, 
          @ld INT = LEN(@delimiter);
 
  WITH a AS
  (
    SELECT [start] = 1,
           [end]   = COALESCE(NULLIF(CHARINDEX(@delimiter, @string, 1), 0), @ll),
           [value] = SUBSTRING(@string, 1, COALESCE(NULLIF(CHARINDEX(@delimiter, @string, 1), 0), @ll) - 1),
           [ord] = 1
     UNION ALL
    SELECT [start] = CONVERT(INT, [end]) + @ld,
           [end]   = COALESCE(NULLIF(CHARINDEX(@delimiter, @string, [end] + @ld), 0), @ll),
           [value] = SUBSTRING(@string, [end] + @ld, COALESCE(NULLIF(CHARINDEX(@delimiter, @string, [end] + @ld), 0), @ll)-[end]-@ld),
           [ord] = [ord] + 1
      FROM a
     WHERE [end] < @ll
  )
  INSERT @items 
  SELECT [ord],[value]
    FROM a
   WHERE LEN([value]) > 0
  OPTION (MAXRECURSION 0);
 
  RETURN;
END;
{% endif %}