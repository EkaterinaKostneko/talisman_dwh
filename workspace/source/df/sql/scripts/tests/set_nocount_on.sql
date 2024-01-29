-- set nocount on;

declare @label varchar(8) = 'multiple';

IF OBJECT_ID(N'{{AF_SC_}}test_set_nocount_on', N'U') IS NULL
CREATE TABLE {{AF_SC_}}test_set_nocount_on(
    [id] [int] identity(1,1) NOT NULL,
  [sourceID] [uniqueidentifier] NOT NULL,
  [runID] [nvarchar] (128) NOT NULL,
    [label] [nchar](128) NULL,
  [dt] [datetime] default getdate() NULL
    );

select * from {{AF_SC_}}test_set_nocount_on;

delete {{AF_SC_}}test_set_nocount_on;

INSERT INTO {{AF_SC_}}test_set_nocount_on
  ([sourceid],
   [runid],
   [label])
SELECT
  N'абв123' sourceid,
  '{{ AF_RUN_ID }}' as runid,
  @label as label;

select * from {{AF_SC_}}test_set_nocount_on;
