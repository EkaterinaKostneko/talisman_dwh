insert into {{ AF_SC_ }}test_basic_table (label)
  select 'context_upload: ' || op_ins_id
    from {{ AF_SDF_ }}entity_ctx
   where op_ins_id = {{ AF_OP_INS_ID }}
   limit 1;