DROP TABLE IF EXISTS core.act_margin_projects ;

CREATE TABLE core.act_margin_projects (
	docdate date NULL,
	pharmacycode varchar NULL,
	typeorder int4 NULL,
	onlinesale int2 NULL,
	orderquantity int4 NULL,
	sallingsum numeric(12, 2) NULL,
	purchasesum numeric(12, 2) NULL
);

DROP TABLE IF EXISTS marts.mart_margin_projects_hot ;

CREATE TABLE marts.mart_margin_projects_hot (
	"Дата" date NULL,
	"Код аптеки проекта" varchar NULL,
	"Код контрагента" varchar NULL,
	"Номер типа заказа" int2 NULL,
	"Тип заказа" varchar NULL,
	"Количество" int4 NULL,
	"Оборот" numeric(12, 2) NULL,
	"Себестоимость" numeric(12, 2) NULL,
	"Наценка" numeric(12, 2) NULL
);

DROP TABLE IF EXISTS marts.mart_total_results_by_store_hot ;

CREATE TABLE marts.mart_total_results_by_store_hot (
	docdate date NULL,
	pharmacycode varchar(10) NULL,
	turnover numeric(12, 4) NULL,
	quanity int4 NULL,
	totalsum numeric(12, 4) NULL,
	bonusessum numeric(12, 4) NULL,
	discountsum numeric(12, 4) NULL,
	individualdiscountsum numeric(12, 4) NULL,
	coupon numeric(12, 4) NULL,
	checksum_purchase numeric(12, 4) NULL,
	checksum_selling numeric(12, 4) NULL,
	checksum_withdiscount numeric(12, 4) NULL,
	intquantity numeric(12, 4) NULL
);

GRANT TRIGGER, REFERENCES, TRUNCATE, INSERT, DELETE, UPDATE, SELECT ON TABLE marts.mart_total_results_by_store_hot TO apteka;
GRANT TRIGGER, REFERENCES, TRUNCATE, INSERT, DELETE, UPDATE, SELECT ON TABLE marts.mart_total_results_by_store_hot TO data_etl;
GRANT TRIGGER, REFERENCES, TRUNCATE, INSERT, DELETE, UPDATE, SELECT ON TABLE marts.mart_total_results_by_store_hot TO df;
GRANT TRIGGER, REFERENCES, TRUNCATE, INSERT, DELETE, UPDATE, SELECT ON TABLE marts.mart_total_results_by_store_hot TO ekostenko;
