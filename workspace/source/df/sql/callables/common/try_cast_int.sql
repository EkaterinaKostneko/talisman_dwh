{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
{% elif AF_DWH_DB_DIALECT|lower == "postgresql" %}

drop function if exists {{ AF_SDF_ }}try_cast_int;

create function {{ AF_SDF_ }}try_cast_int(p_value text, p_default int default null)
   returns int
as
$$
begin
  begin
    return p_value::int;
  exception 
    when others then
       return p_default;
  end;
end;
$$
language plpgsql;

{% endif %}