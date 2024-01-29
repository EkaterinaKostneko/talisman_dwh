{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
{% elif AF_DWH_DB_DIALECT|lower == "postgresql" %}

drop function if exists {{ AF_SDF_ }}try_cast_float;

create function {{ AF_SDF_ }}try_cast_float(p_value text, p_default float default null)
   returns float
as
$$
begin
  begin
    return p_value::float;
  exception 
    when others then
       return p_default;
  end;
end;
$$
language plpgsql;

{% endif %}