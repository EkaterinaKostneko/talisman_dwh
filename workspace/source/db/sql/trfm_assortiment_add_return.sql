TRUNCATE TABLE core.assortiment_plain_add_return;

INSERT INTO core.assortiment_plain_add_return
SELECT
    docdate,
    iddoc,
    lineno ,
    updatedate ,
    idstore ,
    iditem ,
    -return_quantity AS quantity ,
    0 AS return_quantity,
    move_quantity ,
    -revenue/quantity*return_quantity  AS revenue,
    -purchase/quantity*return_quantity AS purchase ,
    -(discount1 +
    discount2 +
    discount3 +
    discount4 +
    discount5)/quantity*return_quantity AS discount,
    manufacture ,
    kind
FROM
    core.assortiment_plain ap
WHERE
    return_quantity <>0
    AND quantity <> 0
UNION ALL
SELECT
    docdate,
    iddoc,
    lineno ,
    updatedate ,
    idstore ,
    iditem ,
    -return_quantity AS quantity ,
    0 AS return_quantity,
    move_quantity ,
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