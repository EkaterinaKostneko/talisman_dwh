{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
IF object_id('{{ AF_SDF_ }}to_log', 'P') IS NOT null
DROP PROCEDURE {{ AF_SDF_ }}to_log;

CREATE PROCEDURE {{ AF_SDF_ }}to_log 
  @op_ins_id int = null,
  @caller varchar(128),
  @message nvarchar(256), 
  @description nvarchar(1024) = null,
  @type nvarchar(32) = null,
  @value nvarchar(256) = null,
  @data nvarchar(max) = null,
  @jdata nvarchar(max) = null,
  @level varchar(10) = 'INFO'
AS
BEGIN
  BEGIN TRY
    BEGIN TRANSACTION
    INSERT INTO {{ AF_SDF_ }}log (op_ins_id, caller, message, description, type, value, data, jdata, level)
    VALUES (@op_ins_id, @caller, @message, @description, @type, @value, @data, @jdata, @level)
    COMMIT TRANSACTION
  END TRY
  BEGIN CATCH
    IF @@trancount > 0 ROLLBACK TRANSACTION
    DECLARE @err_message nvarchar(2048) = error_message()
    RAISERROR(@err_message, 16, 1)
    RETURN
  END CATCH
END;

{% elif AF_DWH_DB_DIALECT|lower == "postgresql" %}

drop procedure if exists {{ AF_SDF_ }}to_log;

create procedure {{ AF_SDF_ }}to_log(
    p_op_ins_id bigint default null,
    p_caller varchar default null,
    p_message varchar default null,
    p_description varchar default null,
    p_type varchar default null,
    p_value varchar default null,
    p_data varchar default null,
    p_jdata json default null,
    p_level varchar default 'INFO'
)
language plpgsql as $$
declare
    v_sql varchar;
    v_connname varchar = 'logging';
begin
    v_sql = format('insert into {{ AF_SDF_ }}"log"("op_ins_id", "caller", "message", "description", "type", "value", "data", "jdata", "level")
                   values (%L, %L, %L, %L, %L, %L, %L, %L, %L)',
                   p_op_ins_id, p_caller, p_message, p_description, p_type, p_value, p_data, p_jdata, p_level);
                   
    perform {{ AF_SDF_ }}dblink_connect_u(v_connname, 'dbname=' || current_database());
    perform {{ AF_SDF_ }}dblink_exec(v_connname, v_sql);
    perform {{ AF_SDF_ }}dblink_disconnect(v_connname);
end
$$;
{% endif %}