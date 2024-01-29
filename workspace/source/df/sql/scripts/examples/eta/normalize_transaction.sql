insert into {{ AF_SC_ }}test_basic_table (label)
  select top 1 'normalize_transaction: ' + cast(op_ins_id as nchar(128))
    from {{ AF_SDF_ }}entity_ctx
   where op_ins_id = {{ AF_OP_INS_ID }};