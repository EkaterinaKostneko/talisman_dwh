{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
{% elif AF_DWH_DB_DIALECT|lower == "postgresql" %}

drop function if exists {{ AF_SDF_ }}try_cast_dt;

create function {{ AF_SDF_ }}try_cast_dt(p_value text, p_default date default null)
   returns date
as
$$
begin
  begin
    return p_value::date;
  exception 
    when others then
       return p_default;
  end;
end;
$$
language plpgsql;

{% endif %}
