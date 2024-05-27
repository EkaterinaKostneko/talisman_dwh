delete from ods.dt21203_rr
where date_time_iddoc between '{{ AF_INC_BEGIN }}' and '{{ AF_INC_END }}';

INSERT INTO ods.dt21203_rr
(
	iddoc,
	date_time_iddoc,
	lineno_,
	sp21207,
	sp21209,
	sp21210,
	sp25099,
	sp21211,
	sp21451,
	sp21226,
	sp23196,
	sp25076,
	sp25077,
	sp25078,
	sp25079,
	sp25080,
	sp25081,
	sp25082,
	sp25083,
	sp25084,
	sp25085,
	sp25086
)
SELECT
	iddoc,
	date_time_iddoc,
	lineno_,
	sp21207,
	sp21209,
	sp21210,
	sp25099,
	sp21211,
	sp21451,
	sp21226,
	sp23196,
	sp25076,
	sp25077,
	sp25078,
	sp25079,
	sp25080,
	sp25081,
	sp25082,
	sp25083,
	sp25084,
	sp25085,
	sp25086
FROM stg_dwh.dt21203_rr;
