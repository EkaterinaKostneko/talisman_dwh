INSERT INTO ods.dt21203_retail_reports
(
	DocTabl.iddoc,
	sp21207,
	sp21226,
	sp21209,
	sp25086
)
SELECT
	DocTabl.iddoc,
	sp21207,
	sp21226,
	sp21209,
	sp25086
FROM stg_dwh.dt21203_retail_reports;
