{% if AF_DWH_DB_DIALECT|lower == "postgresql" %}

drop function if exists {{ AF_SDF_ }}must_be_not_null;

create function {{ AF_SDF_ }}must_be_not_null(value text)
   returns boolean
as
$$
declare
  res boolean := true;
begin
  if value is null then
    res := false;
  end if;
  return res;
end;
$$
language plpgsql;

{% endif %}