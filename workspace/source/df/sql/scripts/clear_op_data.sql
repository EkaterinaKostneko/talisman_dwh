DELETE FROM {{AF_DWH_DB_SCHEMA}}.op_data od
WHERE (od.expiration IS NULL AND od.created < CURRENT_TIMESTAMP - INTERVAL '{{AF_DEFAULT_OP_DATA_RETENTION_IN_DAYS}}' DAY)
OR (od.expiration IS NOT NULL AND od.expiration < CURRENT_TIMESTAMP);
{% if AF_RUN_VACUUM|lower == "true" %}
VACUUM {{AF_DWH_DB_SCHEMA}}.op_data;
{% endif %}