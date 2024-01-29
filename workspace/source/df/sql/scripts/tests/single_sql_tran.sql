if object_id(N'tuser.test_single_sql_tran', N'U') IS NOT NULL
drop table tuser.test_single_sql_tran;

create table tuser.test_single_sql_tran(id int identity(1,1), label varchar(10)
primary key clustered (id)
);

create table #test_single_sql_tran(label varchar(10));

insert into #test_single_sql_tran(label) values('test_1');

insert into tuser.test_single_sql_tran(label)
select label from #test_single_sql_tran;