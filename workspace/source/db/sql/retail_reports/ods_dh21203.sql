delete from ods.dh21203_rr
where sp21391::date between '{{ AF_INC_BEGIN }}' and '{{ AF_INC_END }}';

INSERT INTO ods.dh21203_rr
(
	iddoc,
	sp21204,
	sp21205,
	sp26451,
	sp21221,
	sp21391,
	sp21473,
	sp21474,
	sp21475,
	sp21476,
	sp25183,
	sp21477,
	sp21478,
	sp21571,
	sp22246,
	sp22247,
	sp21570,
	sp22248,
	sp24234,
	sp25529,
	sp22291,
	sp22322,
	sp22659,
	sp22635,
	sp22636,
	sp23197,
	sp23390,
	sp23745,
	sp24235,
	sp24236,
	sp24237,
	sp24238,
	sp24239,
	sp24240,
	sp24241,
	sp24242,
	sp24243,
	sp21209,
	sp21210,
	sp25099,
	sp21226,
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
	sp25086,
	sp20962,
	sp21416,
	sp21466,
	sp475
)
SELECT
	iddoc,
	sp21204,
	sp21205,
	sp26451,
	sp21221,
	sp21391,
	sp21473,
	sp21474,
	sp21475,
	sp21476,
	sp25183,
	sp21477,
	sp21478,
	sp21571,
	sp22246,
	sp22247,
	sp21570,
	sp22248,
	sp24234,
	sp25529,
	sp22291,
	sp22322,
	sp22659,
	sp22635,
	sp22636,
	sp23197,
	sp23390,
	sp23745,
	sp24235,
	sp24236,
	sp24237,
	sp24238,
	sp24239,
	sp24240,
	sp24241,
	sp24242,
	sp24243,
	sp21209,
	sp21210,
	sp25099,
	sp21226,
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
	sp25086,
	sp20962,
	sp21416,
	sp21466,
	sp475
FROM stg_dwh.dh21203_rr;