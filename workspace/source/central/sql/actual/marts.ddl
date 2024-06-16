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
