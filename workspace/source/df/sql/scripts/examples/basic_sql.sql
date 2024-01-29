{% set dialect = AF_DWH_DB_DIALECT|lower %}
{% if dialect == "mssql" %}
insert into tuser.test_basic_table(label) values('basic_sql_ms');
{% elif dialect == "postgresql" %}
insert into test_basic_table(label) values('basic_sql_postgres');
{% else %}
insert into test_basic_table(label) values('{{ AF_DWH_DB_DIALECT|lower }}');
{% endif %}
