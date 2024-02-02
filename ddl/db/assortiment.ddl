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






