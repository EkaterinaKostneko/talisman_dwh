set nocount on;

declare @label varchar(8) = 'multiple';

IF OBJECT_ID(N'{{AF_SC_}}test_swallowed_error', N'U') IS NULL   
CREATE TABLE {{AF_SC_}}test_swallowed_error(
    [id] [int] identity(1,1) NOT NULL,
  [sourceID] [uniqueidentifier] NOT NULL,
  [runID] [nvarchar] (128) NOT NULL,
    [label] [nchar](128) NULL,
  [dt] [datetime] default getdate() NULL
    );

select * from {{AF_SC_}}test_swallowed_error;

delete {{AF_SC_}}test_swallowed_error;

INSERT INTO {{AF_SC_}}test_swallowed_error
  ([sourceid],
   [runid],
   [label])
SELECT
  N'абв123' sourceid,
  '{{ AF_RUN_ID }}' as runid,
  @label as label;

select * from {{AF_SC_}}test_swallowed_error;
