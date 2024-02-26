DROP TABLE IF EXISTS stg_dwh.SC24313_doctor;
CREATE TABLE stg_dwh.SC24313_doctor (
	ROW_ID          int ,
	ID              varchar(9),
	CODE            varchar(8),
	DESCR           varchar(25),
	PARENTEXT       varchar(9),
	ISMARK          boolean,
	VERSTAMP        integer,
	SP24316         varchar(18),
	SP24315         varchar(10),
	SP24317         varchar(9),
	SP24318         numeric(9,2),
	SP24319         timestamp,
	SP24320         numeric(7,2),
	SP24335         numeric(7,2)
);

DROP TABLE IF EXISTS stg_dwh.sc24297_receipt_bonus;
CREATE TABLE stg_dwh.sc24297_receipt_bonus (
	ROW_ID          int ,
	ID              varchar(9) ,
	PARENTID        varchar(9) ,
	CODE            varchar(10) ,
	DESCR           varchar(40) ,
	ISFOLDER        int2 ,
	ISMARK          boolean ,
	VERSTAMP        integer ,
	SP24299         timestamp ,
	SP24300         varchar(20) ,
	SP24301         varchar(100) ,
	SP24302         numeric(12,0) ,
	SP24303         numeric(19,0) ,
	SP24304         varchar(9) ,
	SP24307         numeric(16,0) ,
	SP24349         timestamp ,
	SP24350         varchar(9) ,
	SP24407         varchar(9)
);

DROP TABLE IF EXISTS stg_dwh.sc208_staff;
CREATE TABLE stg_dwh.sc208_staff (
	ROW_ID      int  ,
	ID          varchar(9),
	PARENTID    varchar(9),
	CODE        varchar(6),
	DESCR       varchar(50),
	ISFOLDER    int2 ,
	ISMARK      boolean ,
	VERSTAMP    int
);