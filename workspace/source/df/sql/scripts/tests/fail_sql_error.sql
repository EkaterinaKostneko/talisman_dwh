if object_id(N'tuser.test_no_error_fk', N'U') IS NOT NULL
drop table tuser.test_no_error_fk;

if object_id(N'tuser.test_no_error', N'U') IS NOT NULL
drop table tuser.test_no_error;

create table tuser.test_no_error(id int identity(1,1), label varchar(10)
primary key clustered (id)
);

create table tuser.test_no_error_fk(id int identity(1,1), label varchar(10), error_id int);

alter table tuser.test_no_error_fk with check add constraint fk_no_error foreign key (error_id)
references test_no_error (id);

insert into tuser.test_no_error(label) values('test_1');
insert into tuser.test_no_error_fk(label, error_id) values('test_fk_2', 1);

delete tuser.test_no_error where label = 'test_1';