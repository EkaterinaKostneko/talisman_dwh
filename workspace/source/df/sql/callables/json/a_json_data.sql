{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
/*
    Jose Segarra
    https://www.codeproject.com/Articles/1000953/JSON-for-Sql-Server-Part
*/
IF OBJECT_ID('{{ AF_SDF_ }}json_to_json', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}json_to_json
;

IF OBJECT_ID('{{ AF_SDF_ }}json_value', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}json_value
;

IF OBJECT_ID('{{ AF_SDF_ }}json_value2', 'FN') IS NOT null
DROP FUNCTION {{ AF_SDF_ }}json_value2
;

IF type_id('{{ AF_SDF_ }}json_data') IS NOT NULL
DROP TYPE {{ AF_SDF_ }}json_data
;

CREATE TYPE {{ AF_SDF_ }}json_data AS TABLE(
    [id] [int] NOT NULL,
    [parent] [int] NOT NULL,
    [name] [nvarchar](100) NOT NULL,
    [kind] [nvarchar](10) NOT NULL,
    [value] [nvarchar](max) NOT NULL
);

{% endif %}
