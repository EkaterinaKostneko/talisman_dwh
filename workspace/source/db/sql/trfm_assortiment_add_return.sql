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

CREATE INDEX assortiment_plain_docdate_idx2 ON core.assortiment_plain_add_return USING btree (docdate);
CREATE INDEX assortiment_plain_iddoc_idx2 ON core.assortiment_plain_add_return USING btree (iddoc);
CREATE INDEX assortiment_plain_iditem_idx2 ON core.assortiment_plain_add_return USING btree (iditem);
CREATE INDEX assortiment_plain_idstore_idx2 ON core.assortiment_plain_add_return USING btree (idstore);
CREATE INDEX assortiment_plain_updatedate_idx2 ON core.assortiment_plain_add_return USING btree (updatedate);

INSERT INTO core.assortiment_plain_add_return
--возвраты, если количество товара <> 0
SELECT
    docdate,
    iddoc,
    lineno ,
    updatedate ,
    idstore ,
    iditem ,
    -return_quantity AS quantity ,
    0 AS return_quantity,
    0 AS move_quantity ,
    -revenue/quantity*return_quantity  AS revenue,
    -purchase/quantity*return_quantity AS purchase ,
--    -(discount1 +
--    discount2 +
--    discount3 +
--    discount4 +
--    discount5)/quantity*return_quantity AS discount,
    0 as discount,
    manufacture ,
    kind
FROM
    core.assortiment_plain ap
WHERE
    return_quantity <>0
    AND quantity <> 0
UNION ALL
--возвраты, если количество = 0
SELECT
    docdate,
    iddoc,
    lineno ,
    updatedate ,
    idstore ,
    iditem ,
    -return_quantity AS quantity ,
    0 AS return_quantity,
    0 AS move_quantity ,
    -revenue AS revenue,
    -purchASe AS purchASe ,
    -(discount1 +
    discount2 +
    discount3 +
    discount4 +
    discount5) AS discount,
    manufacture ,
    kind
FROM
    core.assortiment_plain ap
WHERE
    return_quantity <>0
    AND quantity = 0        --если нет количества, выручка отражается за весь возврат
UNION ALL
--перемещения, где количество <> 0
SELECT
    docdate,
    iddoc,
    lineno ,
    updatedate ,
    idstore ,
    iditem ,
    -move_quantity AS quantity ,
    0 AS return_quantity,
    0 AS move_quantity ,
    -revenue/quantity*move_quantity  AS revenue,
    -purchase/quantity*move_quantity AS purchase ,
    0 as discount,
    manufacture ,
    kind
FROM
    core.assortiment_plain ap
WHERE
    move_quantity <>0
    AND quantity <> 0