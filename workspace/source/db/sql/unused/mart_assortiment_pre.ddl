-- marts.mart_assortiment_sales_pre definition

-- Drop table

-- DROP TABLE marts.mart_assortiment_sales_pre;

CREATE TABLE marts.mart_assortiment_sales_pre (
	"Дата" date NULL,
	"ID аптеки" varchar(9) NULL,
	"ID товара" varchar(9) NULL,
	"Количество" int4 NULL,
	"Сумма" numeric(12, 2) NULL,
	"СуммаЗакупа" numeric(12, 2) NULL,
	"ID продукта" varchar(9) NULL,
	"СуммаСкидок" numeric(12, 2) NULL
);