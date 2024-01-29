-- stg_dwh.dh21203_retail_reports definition

-- Drop table

DROP TABLE IF EXISTS stg_dwh.dh21203_retail_reports;

CREATE TABLE stg_dwh.dh21203_retail_reports (
	iddoc varchar(9) NULL,
	sp21204 varchar(9) NULL,
	sp21205 numeric(1) NULL,
	sp26451 numeric(1) NULL,
	sp21221 numeric(13, 2) NULL,
	sp21391 timestamp NULL,
	sp21473 numeric(13, 2) NULL,
	sp21474 numeric(13, 2) NULL,
	sp21475 numeric(13, 2) NULL,
	sp21476 numeric(13, 2) NULL,
	sp25183 numeric(13, 2) NULL,
	sp21477 numeric(13, 2) NULL,
	sp21478 numeric(13, 2) NULL,
	sp21571 varchar(9) NULL,
	sp22246 numeric(13, 2) NULL,
	sp22247 numeric(13, 2) NULL,
	sp21570 numeric(13, 2) NULL,
	sp22248 numeric(13, 2) NULL,
	sp24234 numeric(13, 2) NULL,
	sp25529 numeric(10) NULL,
	sp22291 numeric(1) NULL,
	sp22322 numeric(13, 2) NULL,
	sp22659 numeric(10) NULL,
	sp22635 varchar(3) NULL,
	sp22636 numeric(8) NULL,
	sp23197 numeric(13, 2) NULL,
	sp23390 numeric(13, 2) NULL,
	sp23745 numeric(13, 2) NULL,
	sp24235 numeric(13, 2) NULL,
	sp24236 numeric(13, 2) NULL,
	sp24237 numeric(13, 2) NULL,
	sp24238 numeric(13, 2) NULL,
	sp24239 numeric(13, 2) NULL,
	sp24240 numeric(13, 2) NULL,
	sp24241 numeric(13, 2) NULL,
	sp24242 numeric(13, 2) NULL,
	sp24243 numeric(13, 2) NULL,
	sp21209 numeric(13, 4) NULL,
	sp21210 numeric(13, 4) NULL,
	sp25099 numeric(13, 4) NULL,
	sp21226 numeric(13, 2) NULL,
	sp25076 numeric(13, 2) NULL,
	sp25077 numeric(13, 2) NULL,
	sp25078 numeric(13, 2) NULL,
	sp25079 numeric(13, 2) NULL,
	sp25080 numeric(13, 2) NULL,
	sp25081 numeric(9, 3) NULL,
	sp25082 numeric(9, 3) NULL,
	sp25083 numeric(9, 3) NULL,
	sp25084 numeric(9, 3) NULL,
	sp25085 numeric(9, 3) NULL,
	sp25086 numeric(13, 2) NULL,
	sp20962 numeric(7) NULL,
	sp21416 numeric(1) NULL,
	sp21466 varchar(9) NULL,
	sp475 text NULL
);
CREATE INDEX dh21203_retail_reports_iddoc_idx ON stg_dwh.dh21203_retail_reports USING btree (iddoc);

DROP TABLE IF EXISTS stg_dwh.dt21203_retail_reports;

CREATE TABLE stg_dwh.dt21203_retail_reports (
	iddoc varchar(9) NULL,
    lineno_ smallint ,
    SP21207 varchar(9) ,
    SP21209 numeric(13,4)  NULL,
    SP21210 numeric(13,4)  NULL,
    SP25099 numeric(13,4)  NULL,
    SP21211 numeric(13,2)  NULL,
    SP21451 numeric(13,2)  NULL,
    SP21226 numeric(13,2)  NULL,
    SP23196 numeric(14,0)  NULL,
    SP25076 numeric(13,2)  NULL,
    SP25077 numeric(13,2)  NULL,
    SP25078 numeric(13,2)  NULL,
    SP25079 numeric(13,2)  NULL,
    SP25080 numeric(13,2)  NULL,
    SP25081 numeric(9,3)  NULL,
    SP25082 numeric(9,3)  NULL,
    SP25083 numeric(9,3)  NULL,
    SP25084 numeric(9,3)  NULL,
    SP25085 numeric(9,3)  NULL,
    SP25086 numeric(13,2)  NULL,
    );

);
CREATE INDEX dt21203_retail_reports_iddoc_idx ON stg_dwh.dt21203_retail_reports USING btree (iddoc);