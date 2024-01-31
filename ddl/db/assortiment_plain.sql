-- stg_dwh.dh21203_retail_reports definition

-- Drop table

DROP TABLE IF EXISTS core.assortiment_plain;

CREATE TABLE core.assortiment_plain (
	DocDate date,
	IDdoc varchar(9),
	lineno smallint,
	UpdateDate date,
	IDstore varchar(9),
	IDitem varchar(9),
	quantity int4 NULL,
	return_quantity int4,
	move_quantity int4,
	revenue numeric(12, 2),
	purchase numeric(12, 2),
    discount1 numeric(12, 2),
    discount2 numeric(12, 2),
    discount3 numeric(12, 2),
    discount4 numeric(12, 2),
    discount5 numeric(12, 2),
    manufacture numeric(12, 2),
    kind int2
);


CREATE INDEX assortiment_plain_docdate_idx ON core.assortiment_plain USING btree (docdate);
CREATE INDEX assortiment_plain_iddoc_idx ON core.assortiment_plain USING btree (iddoc);
CREATE INDEX assortiment_plain_iditem_idx ON core.assortiment_plain USING btree (iditem);
CREATE INDEX assortiment_plain_idstore_idx ON core.assortiment_plain USING btree (idstore);
CREATE INDEX assortiment_plain_updatedate_idx ON core.assortiment_plain USING btree (updatedate);
CREATE INDEX assortiment_plain_quantity_idx ON core.assortiment_plain (quantity)
    WHERE quantity = 0;


DROP TABLE IF EXISTS core.assortiment_plain_add_return CASCADE;

CREATE table core.assortiment_plain_add_return
AS
SELECT *
FROM core.assortiment_plain
WHERE 1=0;
ALTER TABLE core.assortiment_plain_add_return RENAME COLUMN discount1 TO discount;
ALTER TABLE core.assortiment_plain_add_return DROP COLUMN discount2;
ALTER TABLE core.assortiment_plain_add_return DROP COLUMN discount3;
ALTER TABLE core.assortiment_plain_add_return DROP COLUMN discount4;
ALTER TABLE core.assortiment_plain_add_return DROP COLUMN discount5;

-- core.assortiment_plain_step2 source

CREATE OR REPLACE VIEW core.assortiment_plain_step2
AS SELECT
    assortiment_plain.docdate,
    assortiment_plain.iddoc,
    assortiment_plain.lineno,
    assortiment_plain.updatedate,
    assortiment_plain.idstore,
    assortiment_plain.iditem,
    assortiment_plain.quantity,
    assortiment_plain.return_quantity,
    assortiment_plain.move_quantity,
    assortiment_plain.revenue,
    assortiment_plain.purchase,
    assortiment_plain.discount1 +
    assortiment_plain.discount2 +
    assortiment_plain.discount3 +
    assortiment_plain.discount4 +
    assortiment_plain.discount5 AS discount,
    assortiment_plain.manufacture,
    assortiment_plain.kind
   FROM core.assortiment_plain;

CREATE OR REPLACE VIEW core.assortiment_plain_step3
AS
SELECT * FROM core.assortiment_plain_add_return
UNION ALL
SELECT * FROM core.assortiment_plain_step2;

CREATE OR REPLACE VIEW core.assortiment_final
AS
SELECT *
FROM core.assortiment_plain_step3;