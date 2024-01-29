{% set SYS_ = AF_SC_SYS_ %}
{% set RAW_ = AF_SC_RAW_ %}
{% set DDS_ = AF_SC_DDS_ %}
{% if AF_DWH_DB_DIALECT|lower == "mssql" %}
  set xact_abort on
  set nocount on

  /* Переменные */
  declare @sql nvarchar(max),
          --
          @p_tab nvarchar(128),
          @p_tab_path nvarchar(128),
          @c_tab nvarchar(128),
          @c_tab_path nvarchar(128),
          @tmp_tab_name nvarchar(128),
          @tmp_tab_path nvarchar(128),
          @err_tab nvarchar(128),
          @err_tab_path nvarchar(128),
          --
          @cols varchar(max),
          @t_cols varchar(max),
          @create_cols varchar(max),
          @create_t_cols varchar(max),
          @p_cols varchar(max),
          @c_col_expr varchar(max),
          @c_cols varchar(max),
          @c_t_code varchar(max),
          @c_t_cols varchar(max),
          @cmn_tag_cond varchar(max),
          @select_e varchar(max),
          --
          @p_id varchar(128),
          @c_id varchar(128),
          @p_runid varchar(128),
          @c_runid varchar(128),
          @c_tag varchar(128),
          @c_sid varchar(128),
          --
          @id int,  
          @tableName varchar(128), 
          @tableRole varchar(32), 
          @colName varchar(64), 
          @colType varchar(32), 
          @colPrimaryKey bit, 
          @colLogicalKey bit,
          @colLength varchar(32),
          @colUnique bit,
          @colNullable bit,
          @colFormat nvarchar(1024),
          @colRole varchar(32),
          --
          @err_message nvarchar(2048),
          @err_severity tinyint,
          @err_state tinyint,
          @err_line int,
          --
          @log_message nvarchar(2048),
          @log_data nvarchar(max),
          @log_value nvarchar(256),
          --
          @ruleRunId int,
          @error_count int,
          --
          @concat_str nvarchar(max)
          
  /* Константы */
  declare @taskId varchar(128) = '{{ AF_TASK_ID }}',
          @runId varchar(128) = '{{ AF_RUN_ID }}',
          @jobId int = {{ AF_JOB_ID }},
          --
          @log_level varchar(32) = case when '{{ AF_LOGLEVEL }}' = 'debug' 
                                        then 'DEBUG'
                                        else 'INFO'
                                   end,
          @log_type nvarchar(32) = 'rows', 
          --
          @caller varchar(32) = 'typify_audit',
          --
          @err_many_ent nvarchar(256) =  'Найдено более одной сущности с ролью %s',
          @err_missing_ent nvarchar(256) =  'Не найдена сущность с ролью %s',
          @err_many_attr nvarchar(256) = 'Найдено более одного атрибута с ролью %s в сущности с ролью %s',
          @err_missing_attr nvarchar(256) =  'Не найден атрибут с ролью %s в сущности с ролью %s',
          --
          @err_tab_default nvarchar(128) = 'df_error',
          --
          @LF nchar(2) = char(10)

  /* Курсоры */
  declare cur cursor local fast_forward for
   select c.id, c.tableName, c.tableRole, c.colName, c.colType, c.colPrimaryKey, c.colLogicalKey, c.colLength, 
          c.colUnique, c.colNullable, c.colFormat, c.colRole
     from {{ SYS_ }}df_entity_ctx p
    inner join {{ SYS_ }}df_entity_ctx c
       on (c.colName = p.colName and isnull(p.colRole, c.colRole) is null or c.colRole = p.colRole)
      and c.tableRole = 'consumer' 
      and substring(reverse(c.tableName), 1, 2) = 't_'
      and c.jobId = @jobId
    where p.tableRole = 'producer' 
      and p.jobId = @jobId

  select distinct 
         @p_tab = tableName,
         @p_tab_path = '{{ DWH_ }}' + tableName  
    from {{ SYS_ }}df_entity_ctx 
   where tableRole = 'producer' 
     and jobId = @jobId

  if @@rowcount > 1 
  begin
    raiserror(@err_many_ent, 16, 1, 'producer')
    return 
  end

  if @p_tab is null 
  begin
    raiserror(@err_missing_ent, 16, 1, 'producer')
    return 
  end

  select distinct
         @c_tab = tableName,
         @c_tab_path = '{{ DWH_ }}' + tableName,
         @tmp_tab_name = '##' + tableName + '_tmp',
         @tmp_tab_path = 'tempdb..##' + tableName + '_tmp'
    from {{ SYS_ }}df_entity_ctx 
   where tableRole = 'consumer' 
     and substring(reverse(tableName), 1, 2) = 't_'
     and jobId = @jobId

  if @@rowcount > 1 
  begin
    raiserror(@err_many_ent, 16, 1, 'consumer')
    return 
  end

  if @c_tab is null 
  begin
    raiserror(@err_missing_ent, 16, 1, 'consumer')
    return 
  end
  
  select distinct
         @err_tab = tableName
    from {{ SYS_ }}df_entity_ctx 
   where tableRole = 'consumer' 
     and substring(reverse(tableName), 1, 2) = 'e_'
     and jobId = @jobId

  if @@rowcount > 1 
  begin
    raiserror(@err_many_ent, 16, 1, 'consumer (error)')
    return 
  end
  
  set @err_tab = isnull(@err_tab, @err_tab_default)
  set @err_tab_path = case when @err_tab = @err_tab_default
                           then '{{ SYS_ }}'
                           else '{{ DWH_ }}' 
                      end + @err_tab

  select @p_id = p.colName, 
         @c_id = c.colName
    from {{ SYS_ }}df_entity_ctx c
    left join {{ SYS_ }}df_entity_ctx p
      on p.tableRole = 'producer'
     and p.colRole is not null
     and c.colRole is not null
     and p.colRole = c.colRole
     and p.jobId = c.jobId
   where c.tableRole = 'consumer'
     and substring(reverse(c.tableName), 1, 2) = 't_'
     and c.colRole = 'id'
     and c.jobId = @jobId

  if @@rowcount > 1 
  begin
    raiserror(@err_many_attr, 16, 1, 'id', 'consumer')
    return 
  end
  
  if @c_id is null 
  begin
    raiserror(@err_missing_attr, 16, 1, 'id', 'consumer')
    return 
  end
  
  select @p_runid = p.colName, 
         @c_runid = c.colName
    from {{ SYS_ }}df_entity_ctx c
    left join {{ SYS_ }}df_entity_ctx p
      on p.tableRole = 'producer'
     and p.colRole is not null
     and c.colRole is not null
     and p.colRole = c.colRole
     and p.jobId = c.jobId
   where c.tableRole = 'consumer'
     and substring(reverse(c.tableName), 1, 2) = 't_'
     and c.colRole = 'runid'
     and c.jobId = @jobId

  if @@rowcount > 1 
  begin
    raiserror(@err_many_attr, 16, 1, 'runid', 'consumer')
    return 
  end
  
  if @c_runid is null 
  begin
    raiserror(@err_missing_attr, 16, 1, 'runid', 'consumer')
    return 
  end

  select @c_tag = c.colName 
     from {{ SYS_ }}df_entity_ctx c
    where c.tableRole = 'consumer' 
      and substring(reverse(c.tableName), 1, 2) = 't_'
      and c.colRole = 'tag' 
      and c.jobId = @jobId

  if @@rowcount > 1 
  begin
    raiserror(@err_many_attr, 16, 1, 'tag', 'consumer')
    return 
  end 
  
  if @c_tag is null 
  begin
    raiserror(@err_missing_attr, 16, 1, 'tag', 'consumer')
    return 
  end
  
   select @c_sid = c.colName 
     from {{ SYS_ }}df_entity_ctx c
    where c.tableRole = 'consumer' 
      and substring(reverse(c.tableName), 1, 2) = 't_'
      and c.colRole = 'sid' 
      and c.jobId = @jobId

  if @@rowcount > 1 
  begin
    raiserror(@err_many_attr, 16, 1, 'sid', 'consumer')
    return 
  end 
  
  if @c_tag is null 
  begin
    raiserror(@err_missing_attr, 16, 1, 'sid', 'consumer')
    return 
  end

  set @create_cols = ''
  set @create_t_cols = ''
  set @cols = ''
  set @t_cols = ''
  set @p_cols = ''
  set @c_col_expr = ''
  set @c_cols = ''
  set @c_t_code = ''
  set @c_t_cols = ''
  set @cmn_tag_cond = ''
  set @select_e = ''

  open cur;
    fetch next from cur into @id, @tableName, @tableRole, @colName, @colType, @colPrimaryKey, @colLogicalKey, @colLength, 
                             @colUnique, @colNullable, @colFormat, @colRole
    while @@fetch_status = 0
    begin
    
      if isnull(@colRole, '') not in ('id', 'runid', 'sid', 'tag') -- технические поля исключены из проверки
      begin
        set @create_cols = @create_cols + @colName + ' ' + case when @colType in ('string', 'varchar') then 'varchar(' + isnull(@colLength, 'max') + ') collate database_default'
                                                                when @colType in ('unicode') then 'nvarchar(' + isnull(@colLength, 'max') + ') collate database_default'
                                                                when @colType in ('decimal') then 'decimal(' + isnull(@colLength, '28,10') + ')'
                                                                when @colType in ('date') then 'date'
                                                                when @colType in ('datetime', 'timestamp') then 'datetime'
                                                                when @colType in ('int64', 'integer') then 'int'
                                                                when @colType in ('float') then 'float'
                                                                else '' 
                                                           end + ',' + @LF
                
        set @create_t_cols = @create_t_cols + 't_' + @colName + ' int,' + @LF
        set @cols = @cols + @colName + ', '
        set @t_cols = @t_cols + 't_' + @colName + ', '
        set @p_cols = @p_cols + @colName + ', '
        set @c_col_expr = case when @colType in ('int64', 'integer', 'int') then 
                                 '{{ SYS_ }}df_try_convert_int(' + @colName + ')'
                               when @colType in ('date') then 
                                 '{{ SYS_ }}df_try_convert_dt_ymd(' + @colName + ')'
                               when @colType in ('datetime', 'timestamp') then 
                                 '{{ SYS_ }}df_try_convert_dtm' + @colName + ')'
                               when @colType in ('decimal') then 
                                 '{{ SYS_ }}df_try_convert_dec(' + @colName + ')'
                               when @colType in ('float') then 
                                 '{{ SYS_ }}df_try_convert_float(' + @colName + ')'
                               -- вероятно надо проверять юникод и анси строки раздельно, тогда надо присоединять типы полей producer
                               when @colType in ('string', 'unicode', 'varchar') then 
                                 'case when isnull(' + isnull(@colLength, 'null') + ', len(' + @colName + ')) >= len(' + @colName + ') then ' + @colName + ' end'
                               else 'null'
                         end;
        set @c_cols = @c_cols + @c_col_expr + ' as c_' + @colName + ',' + @LF
        set @c_t_code = case when @colType in ('int64', 'integer') then 1
                             when @colType in ('decimal') then 2
                             when @colType in ('float') then 3
                             -- со строками вероятно надо проверять на юникод, нужно тогда присоединять типы полей producer
                             when @colType in ('string', 'unicode', 'varchar') then 4
                             when @colType in ('date', 'datetime', 'timestamp') then 5
                        end;
        set @c_t_cols = @c_t_cols +  'case when '+ @colName + ' is not null and c_' + @colName + ' is null then ' + @c_t_code + ' else 0 end as t_' + @colName + ',' + @LF
        set @cmn_tag_cond = @cmn_tag_cond + @colName + ' is not null and c_' + @colName + ' is null or' + @LF

        set @select_e = @select_e + 'select @ruleRunId, ''single'', ' + @c_id + ', ''single'', ''' + @colName +''', ' + @LF
        set @select_e = @select_e + 'case t_'+ @colName + ' when 1 then ''int'' when 2 then ''decimal'' when 3 then ''float'' when 4 then ''string'' when 5 then ''date'' end' + @LF
        set @select_e = @select_e + 'from ' + @tmp_tab_name + @LF
        set @select_e = @select_e + 'where ' + @c_tag + ' > 0 and t_'+ @colName + ' > 0 and ' + @c_runid + ' = @runId' + @LF
        set @select_e = @select_e + 'union all' + @LF
      end;

      fetch next from cur into @id, @tableName, @tableRole, @colName, @colType, @colPrimaryKey, @colLogicalKey, @colLength, 
                               @colUnique, @colNullable, @colFormat, @colRole
    end;
  close cur
  deallocate cur

  set @sql = ''; 
  if object_id(@tmp_tab_path, N'U') is not null 
  begin
    set @log_message = '#0 drop _tmp'
    set @sql = 'drop table '+ @tmp_tab_name
    exec sp_executesql @sql
    exec {{ SYS_ }}df_to_log @taskId, @runId, @jobId, @caller, @log_message
  end;
  
  set @log_message = '#1 create _tmp'
  set @sql = ''; 
  set @sql = @sql + 'create table ' + @tmp_tab_name + ' (' + @LF
  set @sql = @sql + @create_cols + @create_t_cols + @c_id + ' int, ' + @c_runid + ' varchar(128), ' + @c_tag + ' int' + @LF
  set @sql = @sql + ')'
  begin try
    exec sp_executesql @sql
    exec {{ SYS_ }}df_to_log @taskId, @runId, @jobId, @caller, @log_message
  end try
  begin catch
    if @@trancount > 0 rollback
    select @err_message = error_message(), @err_severity = error_severity(), @err_state = error_state(), @err_line = error_line()
    set @log_message = @log_message + ': ' + @err_message
    exec {{ SYS_ }}df_to_log @taskId, @runId, @jobId, @caller, @log_message, null, null, null, @sql, 'ERROR'
    raiserror(@log_message, @err_severity, @err_state)
    return
  end catch

  set @log_message = '#2 insert _tmp'
  set @sql = ''
  set @sql = @sql + 'with t as (' + @LF + 'select' + @LF + @p_cols + @c_cols + @p_id + @LF
  set @sql = @sql + 'from ' + @p_tab_path + @LF
  set @sql = @sql + 'where ' + @p_runid + ' = @runId' + @LF + ')' + @LF
  set @sql = @sql + 'insert into '+ @tmp_tab_name + ' (' + @LF
  set @sql = @sql + @cols + @t_cols + @c_id + ', ' + @c_runid + ', ' + @c_tag + @LF + ')' + @LF
  set @sql = @sql + 'select' + @LF + @c_cols + @c_t_cols + @p_id + ', @runId, ' + @LF
  set @sql = @sql + 'case when ' + @LF
  set @concat_str = 'or' + @LF
  set @sql = @sql + {{ SYS_ }}df_trim_right(@cmn_tag_cond, @concat_str) + @LF + 'then 1 else 0 end as ' + @c_tag + @LF
  set @sql = @sql + 'from t'
  begin try
    set dateformat ymd --формат для приведения дат
    exec sp_executesql @sql, N'@runId varchar(128)', @runId = @runId
    set @log_value = cast(@@rowcount as varchar(256))
    set @log_data = case when @log_level = 'DEBUG' then @sql end
    exec {{ SYS_ }}df_to_log @taskId, @runId, @jobId, @caller, @log_message, null, @log_type, @log_value, @log_data, @log_level
  end try
  begin catch
    if @@trancount > 0 rollback
    select @err_message = error_message(), @err_severity = error_severity(), @err_state = error_state(), @err_line = error_line()
    set @log_message = @log_message + ': ' + @err_message
    exec {{ SYS_ }}df_to_log @taskId, @runId, @jobId, @caller, @log_message, null, null, null, @sql, 'ERROR'
    raiserror(@log_message, @err_severity, @err_state)
    return
  end catch
    
  set @log_message = '#3 insert _t'
  set @sql = ''
  set @sql = @sql + 'delete from ' + @c_tab_path + ' where ' + @c_runid + ' = @runId' + @LF 
  set @sql = @sql + 'insert into '+ @c_tab_path + ' (' + @LF + @cols + @c_sid + ', ' + @c_runid + ', ' + @c_tag + @LF + ')' + @LF
  set @sql = @sql + 'select' + @LF + @cols + '@taskId, @runId, ' + @c_tag + @LF + 'from ' + @tmp_tab_name + @LF
  set @sql = @sql + 'where ' + @c_runid + ' = @runId'
  begin try
    exec sp_executesql @sql, N'@runId varchar(128), @taskId varchar(128)', @runId = @runId, @taskId = @taskId
    set @log_value = cast(@@rowcount as varchar(256))
    set @log_data = case when @log_level = 'DEBUG' then @sql end
    exec {{ SYS_ }}df_to_log @taskId, @runId, @jobId, @caller, @log_message, null, @log_type, @log_value, @log_data, @log_level
  end try
  begin catch
    if @@trancount > 0 rollback
    select @err_message = error_message(), @err_severity = error_severity(), @err_state = error_state(), @err_line = error_line()
    set @log_message = @log_message + ': ' + @err_message
    exec {{ SYS_ }}df_to_log @taskId, @runId, @jobId, @caller, @log_message, null, null, null, @sql, 'ERROR'
    raiserror(@log_message, @err_severity, @err_state)
    return
  end catch
  
  set @log_message = '#4 insert df_ruleRun'
  exec @ruleRunId = {{ SYS_ }}df_add_rule_run  
                      @rule_code = 'target_type_convert',
                      @task_id = @taskId, 
                      @run_id = @runId, 
                      @job_id = @jobId, 
                      @producer_table = @p_tab, 
                      @consumer_table = @c_tab , 
                      @error_table = @err_tab
  exec {{ SYS_ }}df_to_log @taskId, @runId, @jobId, @caller, @log_message, null, null, null, null, 'INFO'

  set @log_message = '#5 insert ' + @err_tab
  set @sql =  'insert into ' + @err_tab_path + '(ruleRunId, rowType, row, colType, col, condition)' + @LF
  set @concat_str = 'union all' + @LF
  set @sql = @sql + {{ SYS_ }}df_trim_right(@select_e, @concat_str)  
  begin try
    exec sp_executesql @sql, N'@ruleRunId int, @runId varchar(128)', 
                             @ruleRunId = @ruleRunId, @runId = @runId
    set @error_count = @@rowcount
    set @log_value = cast(@error_count as varchar(256))
    set @log_data = case when @log_level = 'DEBUG' then @sql end
    exec {{ SYS_ }}df_to_log @taskId, @runId, @jobId, @caller, @log_message, null, @log_type, @log_value, @log_data, @log_level
  end try
  begin catch
    if @@trancount > 0 rollback
    select @err_message = error_message(), @err_severity = error_severity(), @err_state = error_state(), @err_line = error_line()
    set @log_message = @log_message + ': ' + @err_message
    exec {{ SYS_ }}df_to_log @taskId, @runId, @caller, @log_message, null, null, null, @sql, 'ERROR'
    raiserror(@log_message, @err_severity, @err_state)
    return
  end catch
  
  set @log_message = '#6 update df_ruleRun'
  exec dbo.df_set_rule_run_state @rule_run_id = @ruleRunId, @error_count = @error_count
  exec {{ SYS_ }}df_to_log @taskId, @runId, @jobId, @caller, @log_message, null, @log_type, @log_value, @log_data, @log_level
{% elif AF_DWH_DB_DIALECT|lower == "postgresql" %}
do language plpgsql $$
declare
    v_sql varchar;
    --
    v_p_tab_qual varchar(128);
    v_c_tab_qual varchar(128);
    v_tmp_tab varchar(128);
    v_err_tab varchar(128);
    v_err_tab_qual varchar(128);
    --
    v_cols varchar = '';
    v_t_cols varchar = '';
    v_create_cols varchar = '';
    v_create_t_cols varchar = '';
    v_p_cols varchar = '';
    v_c_col_expr varchar = '';
    v_c_cols varchar = '';
    v_c_t_code varchar;
    v_c_t_cols varchar = '';
    v_cmn_tag_cond varchar = '';
    v_select_e varchar = '';
    --
    v_p_id varchar(128);
    v_c_id varchar(128);
    v_p_runid varchar(128);
    v_c_runid varchar(128);
    v_c_tag varchar(128);
    v_c_sid varchar(128);
    --
    v_id int;
    v_tableName varchar(128);
    v_tableRole varchar(32);
    v_colName varchar(64);
    v_t_colName varchar(64);
    v_c_colName varchar(64);
    v_colType varchar(32);
    v_colPrimaryKey boolean;
    v_colLogicalKey boolean;
    v_colLength varchar(32);
    v_colUnique boolean;
    v_colNullable boolean;
    v_colFormat varchar(1024);
    v_colRole varchar(32);
    --
    v_count int;
    v_error_count int;
    v_log_message varchar(2048);
    --
    v_ruleRunId int;
    --

    /* Константы */
    v_taskId varchar(128) = '{{ AF_TASK_ID }}';
    v_runId varchar(128) = '{{ AF_RUN_ID }}';
    v_jobId int = {{ AF_JOB_ID }};
    --
    v_log_level varchar(32) = case when '{{ AF_LOGLEVEL }}' = '' then 'info' else '{{ AF_LOGLEVEL }}' end;
    --
    v_use_runid boolean = ('{{ AF_USE_RUNID }}' = 'true');
    v_use_sid boolean = ('{{ AF_USE_RUNID }}' = 'true');
    --
    v_date_format varchar(32) = case when '{{ AF_DATE_FORMAT }}' = '' then null else '{{ AF_DATE_FORMAT }}' end;
    
    v_caller varchar(32) ='typify_audit';
    --
    v_err_missing_job_id varchar(256) = 'Не передан параметр AF_JOB_ID';
    v_err_many_ent varchar(256) = 'Найдено более одной сущности с ролью %s';
    v_err_missing_ent varchar(256) = 'Не найдена сущность с ролью %s';
    v_err_many_attr varchar(256) = 'Найдено более одного атрибута с ролью %s в сущности с ролью %s';
    v_err_missing_attr varchar(256) ='Не найден атрибут с ролью %s в сущности с ролью %s';
    --
    v_err_tab_default varchar(128) = 'df_error';
    --
    _LF_ varchar(1) = chr(10);

    /* Курсоры */
    cur cursor for
     select c.id,
            quote_ident(c."tableName") as "tableName",
            c."tableRole",
            quote_ident(c."colName") as "colName",
            quote_ident('t_' || c."colName") as "t_colName",
            quote_ident('c_' || c."colName") as "c_colName",
            c."colType",
            c."colPrimaryKey",
            c."colLogicalKey",
            c."colLength",
            c."colUnique",
            c."colNullable",
            c."colFormat",
            c."colRole"
       from {{ SYS_ }}df_entity_ctx p
      inner join {{ SYS_ }}df_entity_ctx c
         on (c."colName" = p."colName" and coalesce(p."colRole", c."colRole") is null or c."colRole" = p."colRole")
        and c."tableRole" = 'consumer'
        and substring(reverse(c."tableName"), 1, 2) = 't_'
        and c."jobId" = v_jobId
      where p."tableRole" = 'producer'
        and p."jobId" = v_jobId;
begin
    call {{ SYS_ }}df_check_dependencies('logging');
    
    if v_jobId is null then
        raise '%', v_err_missing_job_id;
    end if;

    select distinct
           '{{ RAW_ }}' || quote_ident("tableName")
      into v_p_tab_qual
      from {{ SYS_ }}df_entity_ctx
     where "tableRole" = 'producer'
       and "jobId" = v_jobId;

    get diagnostics v_count = row_count;
    if v_count > 1 then
        raise '%', format(v_err_many_ent, 'producer');
    end if;

    if v_p_tab_qual is null then
        raise '%', format(v_err_missing_ent, 'producer');
    end if;

    select distinct
           '{{ DDS_ }}' || quote_ident("tableName"),
           quote_ident("tableName" || '_tmp')
      into v_c_tab_qual, v_tmp_tab
      from {{ SYS_ }}df_entity_ctx
     where "tableRole" = 'consumer'
       and substring(reverse("tableName"), 1, 2) = 't_'
       and "jobId" = v_jobId;

    get diagnostics v_count = row_count;
    if v_count > 1 then
        raise '%', format(v_err_many_ent, 'consumer');
    end if;

    if v_c_tab_qual is null then
        raise '%', format(v_err_missing_ent, 'consumer');
    end if;

    select distinct
           quote_ident("tableName")
      into v_err_tab
      from {{ SYS_ }}df_entity_ctx
     where "tableRole" = 'consumer'
       and substring(reverse("tableName"), 1, 2) = 'e_'
       and "jobId" = v_jobId;

    get diagnostics v_count = row_count;
    if v_count > 1 then
        raise '%', format(v_err_many_ent, 'consumer (error)');
    end if;

    v_err_tab = coalesce(v_err_tab, v_err_tab_default);
    v_err_tab_qual = case when v_err_tab = v_err_tab_default
                             then '{{ SYS_ }}'
                             else '{{ DDS_ }}'
                          end || v_err_tab;

    select p."colName",
           c."colName"
      into v_p_id, v_c_id
      from {{ SYS_ }}df_entity_ctx c
      left join {{ SYS_ }}df_entity_ctx p
        on p."tableRole" = 'producer'
       and p."colRole" is not null
       and c."colRole" is not null
       and p."colRole" = c."colRole"
       and p."jobId" = c."jobId"
     where c."tableRole" = 'consumer'
       and substring(reverse(c."tableName"), 1, 2) = 't_'
       and c."colRole" = 'id'
       and c."jobId" = v_jobId;

    get diagnostics v_count = row_count;
    if v_count > 1 then
        raise '%; %', format(v_err_many_attr, 'producer'), format(v_err_many_attr, 'consumer');
    end if;

    if v_p_id is null then
        raise '%', format(v_err_missing_attr, 'id', 'producer');
    end if;

    if v_c_id is null then
        raise '%', format(v_err_missing_attr, 'id', 'consumer');
    end if;

    if v_use_runid then
        select p."colName",
               c."colName"
          into v_p_runid, v_c_runid
          from {{ SYS_ }}df_entity_ctx c
          left join {{ SYS_ }}df_entity_ctx p
            on p."tableRole" = 'producer'
           and p."colRole" is not null
           and c."colRole" is not null
           and p."colRole" = c."colRole"
           and p."jobId" = c."jobId"
         where c."tableRole" = 'consumer'
           and substring(reverse(c."tableName"), 1, 2) = 't_'
           and c."colRole" = 'runid'
           and c."jobId" = v_jobId;

        get diagnostics v_count = row_count;
        if v_count > 1 then
            raise '%; %', format(v_err_many_attr,  'runid', 'producer'), format(v_err_many_attr,  'runid', 'consumer');
        end if;

        if v_p_runid is null then
            raise '%', format(v_err_missing_attr, 'runid', 'producer');
        end if;

        if v_c_runid is null then
            raise '%', format(v_err_missing_attr, 'runid', 'consumer');
        end if;
    end if;

    select c."colName"
      into v_c_tag
      from {{ SYS_ }}df_entity_ctx c
     where c."tableRole" = 'consumer'
       and substring(reverse(c."tableName"), 1, 2) = 't_'
       and c."colRole" = 'tag'
       and c."jobId" = v_jobId;

    get diagnostics v_count = row_count;
    if v_count > 1 then
        raise '%', format(v_err_many_attr, 'tag', 'consumer');
    end if;

    if v_c_tag is null then
        raise '%', format(v_err_missing_attr, 'tag', 'consumer');
    end if;

    if v_use_sid then
        select c."colName"
          into v_c_sid
          from {{ SYS_ }}df_entity_ctx c
         where c."tableRole" = 'consumer'
           and substring(reverse(c."tableName"), 1, 2) = 't_'
           and c."colRole" = 'sid'
           and c."jobId" = v_jobId;

        get diagnostics v_count = row_count;
        if v_count > 1 then
            raise '%', format(v_err_many_attr, 'sid', 'consumer');
        end if;

        if v_c_tag is null then
            raise '%', format(v_err_missing_attr, 'sid', 'consumer');
        end if;
    end if;

    open cur;
    loop
        fetch cur into v_id, v_tableName, v_tableRole, v_colName, v_t_colName, v_c_colName, v_colType, v_colPrimaryKey, v_colLogicalKey, v_colLength,
                       v_colUnique, v_colNullable, v_colFormat, v_colRole;

        exit when not found;

        if coalesce(v_colRole, '') not in ('id', 'runid', 'sid', 'tag') then -- технические поля исключены из проверки
            v_create_cols =
                v_create_cols || v_colName || ' ' ||
                case when v_colType in ('string', 'varchar') then 'varchar' || case when v_colLength is null then '' else '(' || v_colLength ||')' end
                     when v_colType in ('unicode') then 'varchar' || case when v_colLength is null then '' else '(' || v_colLength ||')' end
                     when v_colType in ('decimal') then 'decimal(' || coalesce(v_colLength, '28,10') || ')'
                     when v_colType in ('date') then 'date'
                     when v_colType in ('dateTime', 'timestamp') then 'timestamp'
                     when v_colType in ('int64', 'integer') then 'int'
                     when v_colType in ('float') then 'float'
                     else ''
                end || ',' || _LF_;

            v_create_t_cols = v_create_t_cols || v_t_colName || ' int,' || _LF_;
            v_cols = v_cols || v_colName || ', ';
            v_t_cols = v_t_cols || v_t_colName || ', ';
            v_p_cols = v_p_cols || v_colName || ', ';
            v_c_col_expr =
                case when v_colType in ('int64', 'integer', 'int') then
                          '{{ SYS_ }}df_try_cast_int(' || v_colName || ')'
                     when v_colType in ('date') then
                          '{{ SYS_ }}df_try_cast_dt(' || v_colName || case when v_date_format is null then '' else ', ''' || v_date_format || ''''  end || ')'
                     when v_colType in ('dateTime', 'timestamp') then
                          '{{ SYS_ }}df_try_cast_dtm(' || v_colName || ')'
                     when v_colType in ('decimal') then
                          '{{ SYS_ }}df_try_cast_dec(' || v_colName || ')'
                     when v_colType in ('float') then
                          '{{ SYS_ }}df_try_cast_float(' || v_colName || ')'
                     when v_colType in ('string', 'unicode', 'varchar') then
                          'case when coalesce(' || coalesce(v_colLength, 'null') || ', length(' || v_colName || ')) >= length(' || v_colName || ') then ' || v_colName || ' end'
                     else 'null'
                end;

            v_c_cols = v_c_cols || v_c_col_expr || ' as ' || v_c_colName || ',' || _LF_;

            v_c_t_code =
                case when v_colType in ('int64', 'integer') then '1'
                     when v_colType in ('decimal') then '2'
                     when v_colType in ('float') then '3'
                     when v_colType in ('string', 'unicode', 'varchar') then '4'
                     when v_colType in ('date', 'dateTime', 'timestamp') then '5'
                end;

            v_c_t_cols = v_c_t_cols ||
                'case when ' || v_colName || ' is not null and ' || v_c_colName || ' is null then ' || v_c_t_code || ' else 0 end as ' || v_t_colName || ',' || _LF_;

            v_cmn_tag_cond = v_cmn_tag_cond || v_colName || ' is not null and ' || v_c_colName || ' is null or' || _LF_;

            v_select_e = v_select_e ||
                'select' || _LF_ ||
                    '$1, ''single'', ' ||
                    v_c_id ||
                    ', ''single'', ''' ||
                    v_colName || ''', ' || _LF_ ||
                    'case ' || v_t_colName || ' when 1 then ''int'' when 2 then ''decimal'' when 3 then ''float'' when 4 then ''string'' when 5 then ''date'' end' || _LF_ ||
                'from ' || v_tmp_tab || _LF_ ||
                'where ' || v_c_tag || ' > 0 and ' || v_t_colName || ' > 0' || _LF_ ||
                case when v_use_runid then 'and ' || coalesce(v_c_runid, '') || ' = $2' || _LF_ else '' end ||
                'union all' || _LF_;
        end if;
    end loop;
    close cur;

    v_log_message = '#0 try drop _tmp';
    v_sql = 'drop table if exists ' || v_tmp_tab;

    begin
        execute v_sql;
        call {{ SYS_ }}df_to_log(p_source_id => v_taskId, p_run_id => v_runId, p_job_id => v_jobId, p_caller => v_caller, p_message => v_log_message,
                                 p_description => v_tmp_tab, p_data => case when v_log_level = 'debug' then v_sql end, p_level => v_log_level);
    exception
        when others then
            call {{ SYS_ }}df_to_log(p_source_id => v_taskId, p_run_id => v_runId, p_job_id => v_jobId, p_caller => v_caller, p_message => v_log_message,
                                     p_description => SQLERRM, p_data => v_sql, p_level => 'error');
            raise;
    end;

    v_log_message = '#1 create _tmp';
    v_sql =
        'create temp table ' || v_tmp_tab || ' (' || _LF_ ||
            v_create_cols || v_create_t_cols || v_c_id || ' int, ' || _LF_ ||
            case when v_use_runid then v_c_runid || ' varchar(128), ' || _LF_ else '' end ||
            v_c_tag || ' int' || _LF_ ||
        ')';

    begin
        execute v_sql;
        call {{ SYS_ }}df_to_log(p_source_id => v_taskId, p_run_id => v_runId, p_job_id => v_jobId, p_caller => v_caller, p_message => v_log_message,
                                 p_description => v_tmp_tab, p_data => case when v_log_level = 'debug' then v_sql end, p_level => v_log_level);
    exception
        when others then
            call {{ SYS_ }}df_to_log(p_source_id => v_taskId, p_run_id => v_runId, p_job_id => v_jobId, p_caller => v_caller, p_message => v_log_message,
                                     p_description => SQLERRM, p_data => v_sql, p_level => 'error');
            raise;
    end;

    v_log_message = '#2 insert _tmp';
    v_sql =
        'insert into ' || v_tmp_tab || ' (' || _LF_ ||
            v_cols ||
            v_t_cols || _LF_ ||
            v_c_id || ', ' || _LF_ ||
            case when v_use_runid then v_c_runid || ', ' || _LF_ else '' end ||
            v_c_tag || _LF_ ||
        ')' || _LF_ ||
         'with t as (' || _LF_ ||
            'select' || _LF_ || v_p_cols || v_c_cols || v_p_id || _LF_ ||
             'from ' || v_p_tab_qual || _LF_ ||
             case when v_use_runid then 'where ' || coalesce(v_p_runid, '') || ' = $1' || _LF_ else '' end  ||
        ')' || _LF_ ||
        'select' || _LF_ ||
            v_c_cols ||
            v_c_t_cols ||
            v_p_id || ', ' || _LF_ ||
            case when v_use_runid then '$1, ' || _LF_ else '' end  || --возможно стоит поменять на заполнение значением из producer
            'case when ' || rtrim(v_cmn_tag_cond, 'or' || _LF_) || ' then 1 else 0 end as ' || v_c_tag || _LF_ ||
        'from t';

    begin
        --set dateformat ymd; --формат для приведения дат
        execute v_sql using v_runId;
        get diagnostics v_count = row_count;
        call {{ SYS_ }}df_to_log(p_source_id => v_taskId, p_run_id => v_runId, p_job_id => v_jobId, p_caller => v_caller, p_message => v_log_message,
                                 p_description => v_tmp_tab, p_type => 'row_count', p_value => v_count::varchar,
                                 p_data => case when v_log_level = 'debug' then v_sql end, p_level => v_log_level);
    exception
        when others then
            rollback;
            call {{ SYS_ }}df_to_log(p_source_id => v_taskId, p_run_id => v_runId, p_job_id => v_jobId, p_caller => v_caller, p_message => v_log_message,
                                     p_description => SQLERRM, p_data => v_sql, p_level => 'error');
            raise;
    end;

    v_log_message = '#3 insert _t';
    v_sql =
        'delete from ' || v_c_tab_qual ||
            case when v_use_runid then ' where ' || coalesce(v_c_runid, '') || ' = $1 ' else '' end || ';' || _LF_ ||

        'insert into ' || v_c_tab_qual || ' (' || _LF_ ||
            v_cols || _LF_ ||
            case when v_use_sid then coalesce(v_c_sid, '') || ', ' || _LF_ else '' end ||
            case when v_use_runid then coalesce(v_c_runid, '') || ', ' || _LF_ else '' end ||
            v_c_tag || _LF_ || ')' || _LF_ ||
        'select' || _LF_ ||
            v_cols || _LF_ ||
            case when v_use_sid then '$2, ' || _LF_ else '' end || --возможно стоит поменять на заполнение значением из producer
            case when v_use_runid then '$1, ' || _LF_ else '' end || --возможно стоит поменять на заполнение значением из producer
            v_c_tag || _LF_ ||
        'from ' || v_tmp_tab || _LF_ ||
        case when v_use_runid then 'where ' || coalesce(v_c_runid, '') || ' = $1' else '' end;

    begin
        execute v_sql using v_runId, v_taskId;
        get diagnostics v_count = row_count;
        call {{ SYS_ }}df_to_log(p_source_id => v_taskId, p_run_id => v_runId, p_job_id => v_jobId, p_caller => v_caller, p_message => v_log_message,
                                 p_description => v_c_tab_qual, p_type => 'row_count', p_value => v_count::varchar,
                                 p_data => case when v_log_level = 'debug' then v_sql end, p_level => v_log_level);
    exception
        when others then
            rollback;
            call {{ SYS_ }}df_to_log(p_source_id => v_taskId, p_run_id => v_runId, p_job_id => v_jobId, p_caller => v_caller, p_message => v_log_message,
                                     p_description => SQLERRM, p_data => v_sql, p_level => 'error');
            raise;
    end;

    v_log_message = '#4 add rule run';

     select {{ SYS_ }}df_add_rule_run(p_rule_code => 'target_type_convert', p_task_id => v_taskId, p_run_id => v_runId,  p_job_id => v_jobId,
                                      p_producer_table => v_p_tab_qual, p_consumer_table => v_c_tab_qual, p_error_table => v_err_tab)
       into v_ruleRunId;
    call {{ SYS_ }}df_to_log(p_source_id => v_taskId, p_run_id => v_runId, p_job_id => v_jobId, p_caller => v_caller, p_message => v_log_message,
                             p_type => 'rule_run_id', p_value => v_ruleRunId::varchar);

    v_log_message = '#5 insert _err';
    v_sql =
        'insert into ' || v_err_tab_qual || '("ruleRunId", "rowType", "row", "colType", "col", "condition")' || _LF_ ||
        rtrim(v_select_e, 'union all' || _LF_);

    begin
        execute v_sql using v_ruleRunId, v_runId;
        get diagnostics v_error_count = row_count;
        call {{ SYS_ }}df_to_log(p_source_id => v_taskId, p_run_id => v_runId, p_job_id => v_jobId, p_caller => v_caller, p_message => v_log_message,
                                 p_description => v_err_tab_qual, p_type => 'error_count', p_value => v_error_count::varchar,
                                 p_data => case when v_log_level = 'debug' then v_sql end, p_level => v_log_level);
    exception
        when others then
            rollback;
            call {{ SYS_ }}df_to_log(p_source_id => v_taskId, p_run_id => v_runId, p_job_id => v_jobId, p_caller => v_caller,
                                                   p_description => SQLERRM, p_data => v_sql, p_level => 'error');
            raise;
    end;

    v_log_message = '#6 set rule error count';
    call {{ SYS_ }}df_set_rule_run_state(p_task_id => v_taskId, p_run_id => v_runId, p_job_id => v_jobId, p_rule_run_id => v_ruleRunId, p_error_count => v_error_count);
    call {{ SYS_ }}df_to_log(p_source_id => v_taskId, p_run_id => v_runId, p_job_id => v_jobId, p_caller => v_caller, p_message => v_log_message);
end
$$;
{% endif %}
