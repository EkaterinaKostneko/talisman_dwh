{% if AF_DWH_DB_DIALECT|lower == "postgresql" %}

drop function if exists {{ AF_SDF_ }}must_be_less_than;

create function {{ AF_SDF_ }}must_be_less_than(num text, max_value integer)
   returns boolean
as
$$
declare
  res boolean := true;
begin
  if df.try_cast_int(num, 0) > max_value then
    res := false;
  end if;
  return res;
end;
$$
language plpgsql;

{% endif %}