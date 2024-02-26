CREATE OR REPLACE VIEW marts.mart_assortiment_ce_pretty
AS
SELECT
    docdate     "Дата",
    idstore     "ID аптеки",
    iditem      "ID товара",
    quantity    "Количество",
    revenue-discount
                "Валовая выручка",
    revenue-discount-purchase
                "Валовая прибыль",
    (revenue-discount-purchase)/NULLIF(quantity, 0)
                 "Цена розничная",
    purchase/NULLIF(quantity, 0)
                 "Прибыль с упаковки",
    0
                 "Back маржа",
    purchase/NULLIF(quantity, 0)
                "Общая прибыль с упаковки"
FROM core.assortiment_final af

