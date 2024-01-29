set nocount on;

IF OBJECT_ID(N'test_multiple_statements', N'U') IS NULL   
CREATE TABLE test_multiple_statements(
    [id] [int] identity(1,1) NOT NULL,
	[sourceID] [uniqueidentifier] NOT NULL,
	[runID] [nvarchar] (128) NOT NULL,
    [label] [nchar](128) NULL,
	[dt] [datetime] default getdate() NULL
    );

select * from test_multiple_statements;

delete test_multiple_statements;

INSERT INTO test_multiple_statements
	([sourceid],
	 [runid],
	 [label])
SELECT
	--N'абв123' sourceid, -- uncomment to check conversion error exception catching
  newid() as sourceid, -- comment to check conversion error exception catching
	'{{ AF_RUN_ID }}' as runid,
	'multiple' as label;

select * from test_multiple_statements;
