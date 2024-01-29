{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
{% elif AF_DWH_DB_DIALECT|lower == "postgresql" %}

drop function if exists {{ AF_SDF_ }}try_cast_dec;

create function {{ AF_SDF_ }}try_cast_dec(p_value text, p_default decimal default null, p_precision int default 28, p_scale int default 10)
   returns decimal
as
$$
begin
  begin
    return to_number(p_value, 
                     repeat('9', p_precision - p_scale)||'.'||repeat('9', p_scale));
  exception 
    when others then
       return p_default;
  end;
end;
$$
language plpgsql;
{% endif %}