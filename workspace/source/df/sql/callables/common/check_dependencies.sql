{% if AF_DWH_DB_DIALECT|lower == "mssql" %}

{% elif AF_DWH_DB_DIALECT|lower == "postgresql" %}

drop procedure if exists {{ AF_SDF_ }}check_dependencies;

create procedure {{ AF_SDF_ }}check_dependencies(
    p_type varchar(32)
)
language plpgsql as $$
declare
    v_ext_oid oid;
    v_function_name varchar;
begin
    if p_type = 'logging' then
        begin
            select oid
              into strict v_ext_oid
              from pg_extension
             where extname = 'dblink';
        exception
            when no_data_found then
                raise 'Необходимо установить расширение dblink в схему {{ AF_S_ }}';
        end;
        
        v_function_name = '{{ AF_SDF_ }}dblink_connect_u(text, text)';
        if not has_function_privilege(v_function_name, 'execute') then
            raise 'Необходимо дать пользователю % права execute на функцию %', current_user, v_function_name;
        end if;
    end if;
end
$$;
{% endif %}
