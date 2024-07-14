drop materialized view if exists ods.sales_projects cascade;

create materialized view ods.sales_projects
as
SELECT
--ct.CheckID,
-- дата
ch.DocDate,
-- ID аптеки
ch.PharmacyCode,
-- канал продаж
ct.SalesChannel IDSalesChannel,
-- ID товара
--ct.TovarCode1C,
--ct.TovarName,
-- количество уп./пачек из упаковки(при продаже части упаковки)
Sum(ct.IntQuantity + ct.FracQuantity) Quantity,
-- валовая выручка (до вычета скидок)
Sum(ct.Sum),
-- сумма скидок
Sum(ct.Sk_Akciya + ct.Sk_Zakaz + ct.Sk_Recept + ct.Sk_Bonus + ct.Sk_Okr)  Discounts,
-- валовая выручка (за вычетом скидок)
Sum(ct.Sum-(ct.Sk_Akciya+ct.Sk_Zakaz+ct.Sk_Recept+ct.Sk_Bonus+ct.Sk_Okr))  Revenue,
-- валовая прибыль
Sum(ct.Sum-(ct.Sk_Akciya+ct.Sk_Zakaz+ct.Sk_Recept+ct.Sk_Bonus+ct.Sk_Okr) - ct.PurchasePrice*ct.SellingPrice
	/ct.Price * (ct.IntQuantity + ct.FracQuantity))  GrossProfitSum,
-- цена розничная(до вычета скидок) за 1 уп.
--ct.Price,
-- цена розничная(до вычета скидок) за 1 уп./1 пачку из упаковки(при продаже части упаковки)
--ct.SellingPrice,
-- цена розничная (за вычетом скидок) за 1 уп./1 пачку из упаковки(при продаже части упаковки)
--(ct.Sum-(ct.Sk_Akciya+ct.Sk_Zakaz+ct.Sk_Recept+ct.Sk_Bonus+ct.Sk_Okr))/(ct.IntQuantity + ct.FracQuantity) as DiscountPrice1,
-- прибыль с 1уп./1 пачку из упаковки(при продаже части упаковки)
--(ct.Sum-(ct.Sk_Akciya+ct.Sk_Zakaz+ct.Sk_Recept+ct.Sk_Bonus+ct.Sk_Okr) - ct.PurchasePrice*ct.SellingPrice/ct.Price * (ct.IntQuantity + ct.FracQuantity))/(ct.IntQuantity + ct.FracQuantity) as GrossProfit1,
-- закупочная цена за 1 уп.
--ct.PurchasePrice,
-- закупочная цена за 1 уп./1 пачку из упаковки(при продажи части упаковки)
--ct.PurchasePrice*ct.SellingPrice/ct.Price as PurchasePrice1,
-- закупочная стоимость общая
Sum(ct.PurchasePrice*ct.SellingPrice/ct.Price*(ct.IntQuantity+ct.FracQuantity)) PurchaseSum
from ods.checktables ct
join
ods.CheckHeaders ch
on ch.ID = ct.CheckID
where Status = 1
	AND (ConsumptionType = 1 OR ConsumptionType = 4)
	AND WriteOffFlag = 0
	AND ct.FracQuantity > '0'
--	and ch.DocDate between '2024-06-01' and '2024-06-30'
group by
	ch.pharmacycode ,
	ct.SalesChannel,
	ch.DocDate
	;


drop view if exists marts.mart_sales_projects;

create view marts.mart_sales_projects
as
select
*,
-- канал продаж
(case
 when sp.IDSalesChannel = '0' then 'Офлайн'
 when sp.IDSalesChannel = '1' then 'ТвояАптека.рф'
 when sp.IDSalesChannel = '4' then 'АСЗ'
 when sp.IDSalesChannel = '2' then 'Семейная-аптека.рф'
 else 'Заказ с другой аптеки'
end) as SalesChannel,
(sp.Revenue - sp.PurchaseSum)/ NULLIF(sp.PurchaseSum, 0) Margin
from ods.sales_projects sp