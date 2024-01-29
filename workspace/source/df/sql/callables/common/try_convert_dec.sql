{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
    IF object_id('{{ AF_SDF_ }}try_convert_dec', 'FN') IS NOT null
    DROP FUNCTION {{ AF_SDF_ }}try_convert_dec;

    CREATE FUNCTION {{ AF_SDF_ }}try_convert_dec
    (
      @value nvarchar(4000)
    )
    RETURNS decimal(28,10)
    AS
    BEGIN
      DECLARE @tmp table (col varchar(4000));
      INSERT INTO @tmp VALUES (@value);
      RETURN (SELECT cast('' as xml).value('sql:column("col") cast as xs:decimal ?', 
                                           'decimal(28,10)') 
                FROM @tmp)
    END;
{% endif %}