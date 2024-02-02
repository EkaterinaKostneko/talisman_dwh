CREATE OR REPLACE VIEW marts.mart_assortiment
AS
SELECT
    docdate,
    idstore,
    iditem,
    quantity,
    revenue-discount as rebate,
    revenue-discount-purchase as profit,
    (revenue-discount-purchase)/NULLIF(quantity, 0) as profitpack,
    purchase/NULLIF(quantity, 0) as purchasepack,
    0,
    purchase/NULLIF(quantity, 0) as netpurchasepack
FROM core.assortiment_final af