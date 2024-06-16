-- marts.mart_assortiment_sales definition

-- Drop table

-- DROP TABLE marts.mart_assortiment_sales;

CREATE TABLE marts.mart_assortiment_sales (
	"Дата" date NULL,
	"ID аптеки" bpchar(9) NULL,
	"ID товара" bpchar(9) NULL,
	"Количество" int4 NULL,
	"Валовая выручка" numeric(12, 2) NULL,
	"Валовая прибыль" numeric(12, 2) NULL,
	"Цена розничная" numeric(12, 2) NULL,
	"Прибыль с упаковки" numeric(12, 2) NULL,
	"Back маржа" numeric(12, 2) NULL,
	"Общая прибыль с упаковки" numeric(12, 2) NULL
);
CREATE INDEX "mart_assortiment_sales_id_аптеки_idx" ON marts.mart_assortiment_sales USING btree ("ID аптеки");
CREATE INDEX "mart_assortiment_sales_id_товара_idx" ON marts.mart_assortiment_sales USING btree ("ID товара");
CREATE INDEX "mart_assortiment_sales_дата_idx" ON marts.mart_assortiment_sales USING btree ("Дата");