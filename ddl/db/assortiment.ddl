--чтобы не терять загруженные данные - оставим их в таблице
DROP TABLE IF EXISTS core.assortiment_plain_last;

ALTER TABLE core.assortiment_plain RENAME TO assortiment_plain_last;

CREATE TABLE core.assortiment_plain (
	DocDate             date,
	IDdoc               varchar(9),
	lineno              smallint,
	UpdateDate          date,
	IDstore             varchar(9),
	IDitem              varchar(9),
	quantity            int4,
	return_quantity     int4,
	move_quantity       int4,
	revenue             numeric(12, 2),
	purchase            numeric(12, 2),
    discount1           numeric(12, 2),
    discount2           numeric(12, 2),
    discount3           numeric(12, 2),
    discount4           numeric(12, 2),
    discount5           numeric(12, 2),
    manufacture         numeric(12, 2),
    kind int2
);

CREATE INDEX assortiment_plain_docdate_idx ON core.assortiment_plain USING btree (docdate);
CREATE INDEX assortiment_plain_iddoc_idx ON core.assortiment_plain USING btree (iddoc);
CREATE INDEX assortiment_plain_iditem_idx ON core.assortiment_plain USING btree (iditem);
CREATE INDEX assortiment_plain_idstore_idx ON core.assortiment_plain USING btree (idstore);
CREATE INDEX assortiment_plain_updatedate_idx ON core.assortiment_plain USING btree (updatedate);
CREATE INDEX assortiment_plain_quantity_idx ON core.assortiment_plain (quantity)
    WHERE quantity = 0;

------- таблица с понятной витриной продаж по ассортименту

DROP TABLE marts.mart_assortiment_sales_last;

ALTER TABLE marts.mart_assortiment_sales RENAME TO marts.mart_assortiment_sales_last;

CREATE TABLE marts.mart_assortiment_sales (
	"Дата" date NULL,
	"ID аптеки"             varchar(9),
	"ID товара"             varchar(9),
	"Количество"            int4 ,
	"Валовая выручка"       numeric(12, 2),
	"Валовая прибыль"       numeric(12, 2),
	"Цена розничная"        numeric(12, 2),
	"Прибыль с упаковки"    numeric(12, 2),
	"Back маржа"            numeric(12, 2),
	"Общая прибыль с упаковки" numeric(12, 2)
);

CREATE INDEX "mart_assortiment_sales_id_аптеки_idx" ON marts.mart_assortiment_sales USING btree ("ID аптеки");
CREATE INDEX "mart_assortiment_sales_id_товара_idx" ON marts.mart_assortiment_sales USING btree ("ID товара");
CREATE INDEX "mart_assortiment_sales_дата_idx" ON marts.mart_assortiment_sales USING btree ("Дата");








