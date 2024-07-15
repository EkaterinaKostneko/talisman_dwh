create view marts.mart_assortiment_ce_v2
as
SELECT
ct.CheckID,
-- дата
ch.DocDate,
-- ID аптеки
ch.PharmacyCode,
-- ID товара
ct.TovarCode1C,
ct.TovarName,
-- количество уп./пачек из упаковки(при продаже части упаковки)
ct.IntQuantity + ct.FracQuantity as Quantity,
-- валовая выручка (до вычета скидок)
ct.Sum,
ct.Sk_Akciya, ct.Sk_Zakaz, ct.Sk_Recept, ct.Sk_Bonus, ct.Sk_Okr,
-- сумма скидок
ct.Sk_Akciya + ct.Sk_Zakaz + ct.Sk_Recept + ct.Sk_Bonus + ct.Sk_Okr as Discounts,
-- валовая выручка (за вычетом скидок)
ct.Sum-(ct.Sk_Akciya+ct.Sk_Zakaz+ct.Sk_Recept+ct.Sk_Bonus+ct.Sk_Okr) as "Revenue",
-- валовая прибыль
ct.Sum-(ct.Sk_Akciya+ct.Sk_Zakaz+ct.Sk_Recept+ct.Sk_Bonus+ct.Sk_Okr) - ct.PurchasePrice*ct.SellingPrice/ct.Price * (ct.IntQuantity + ct.FracQuantity) as GrossProfitSum,
-- цена розничная(до вычета скидок) за 1 уп.
ct.Price,
-- цена розничная(до вычета скидок) за 1 уп./1 пачку из упаковки(при продаже части упаковки)
ct.SellingPrice,
-- цена розничная (за вычетом скидок) за 1 уп./1 пачку из упаковки(при продаже части упаковки)
(ct.Sum-(ct.Sk_Akciya+ct.Sk_Zakaz+ct.Sk_Recept+ct.Sk_Bonus+ct.Sk_Okr))/(ct.IntQuantity + ct.FracQuantity) as DiscountPrice1,
-- прибыль с 1уп./1 пачку из упаковки(при продаже части упаковки)
(ct.Sum-(ct.Sk_Akciya+ct.Sk_Zakaz+ct.Sk_Recept+ct.Sk_Bonus+ct.Sk_Okr) - ct.PurchasePrice*ct.SellingPrice/ct.Price * (ct.IntQuantity + ct.FracQuantity))/(ct.IntQuantity + ct.FracQuantity) as GrossProfit1,
-- закупочная цена за 1 уп.
ct.PurchasePrice,
-- закупочная цена за 1 уп./1 пачку из упаковки(при продажи части упаковки)
ct.PurchasePrice*ct.SellingPrice/ct.Price as PurchasePrice1,
-- закупочная стоимость общая
ct.PurchasePrice*ct.SellingPrice/ct.Price*(ct.IntQuantity+ct.FracQuantity) as PurchaseSum,
-- канал продаж
(case
 when ct.SalesChannel = '0' then 'Офлайн'
 when ct.SalesChannel = '1' then 'ТвояАптека.рф'
 when ct.SalesChannel = '4' then 'АСЗ'
 when ct.SalesChannel = '2' then 'Семейная-аптека.рф'
 else 'Заказ с другой аптеки'
end) as SalesChannel
from stg_dwh.act_checktables  ct
join
stg_dwh.act_checkheaders  ch
on ch.ID = ct.CheckID
where ch.Status = 1
AND (ch.ConsumptionType = 1 OR ch.ConsumptionType = 4)
AND ch.writeoffflag = 0
and ch.DocDate >= '2024-06-06'