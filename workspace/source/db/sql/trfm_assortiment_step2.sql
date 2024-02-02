--суммируем все дисконты в одно поле
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