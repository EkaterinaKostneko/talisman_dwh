{% if AF_DWH_DB_DIALECT|lower == "postgresql" %}

drop function if exists {{ AF_SDF_ }}must_be_shorter_than;

create function {{ AF_SDF_ }}must_be_shorter_than(value text, max_length integer)
   returns boolean
as
$$
declare
  res boolean := true;
begin
  if length(value) > max_length then
    res := false;
  end if;
  return res;
end;
$$
language plpgsql;

{% endif %}