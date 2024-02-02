TRUNCATE TABLE marts.mart_assortiment_sales;

INSERT INTO marts.mart_assortiment_sales
(
	"Дата",
 	"ID аптеки",
 	"ID товара",
 	"Количество",
 	"Валовая выручка",
 	"Валовая прибыль",
 	"Цена розничная",
 	"Прибыль с упаковки",
 	"Back маржа",
 	"Общая прибыль с упаковки")
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
