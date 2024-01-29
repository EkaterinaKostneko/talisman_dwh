{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
{% elif AF_DWH_DB_DIALECT|lower == "postgresql" %}

drop function if exists {{ AF_SDF_ }}try_cast_dtm;

create function {{ AF_SDF_ }}try_cast_dtm(p_value text, p_default timestamp default null)
   returns timestamp
as
$$
begin
  begin
    return p_value::timestamp;
  exception 
    when others then
       return p_default;
  end;
end;
$$
language plpgsql;

{% endif %}