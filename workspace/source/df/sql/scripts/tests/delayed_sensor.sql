if object_id(N'test_delayed_sensor', N'U') is not null drop table test_delayed_sensor;
create table test_delayed_sensor(start_time varchar(100), finish_time varchar(500));
insert into test_delayed_sensor(start_time, finish_time)
values('{{ AF_EXECUTION_TIME }}', '{{ AF_NOW }}');