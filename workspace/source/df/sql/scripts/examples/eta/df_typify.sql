create procedure {{ AF_SC_ }}df_typify_{{ AF_JOB_ID }}
as
begin

    insert into tuser.test_basic_table (label)
      select top 1 cast(jobId as nchar(128))
        from tuser.df_entity_ctx
       where jobId = {{ AF_JOB_ID }};

end;
