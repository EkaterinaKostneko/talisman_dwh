insert into {{ AF_SC_ }}test_basic_table (label)
  select top 1 cast(jobId as nchar(128))
    from {{ AF_SC_ }}df_entity_ctx
   where jobId = {{ AF_JOB_ID }};

