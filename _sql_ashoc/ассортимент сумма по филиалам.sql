explain
select
docdate,
ms."Филиал",
revenue - purchase - discount1 - discount2 -discount3 - discount4 -discount5 as profit_net,
sar.additional_products
from
core.assortiment_plain ap,
marts.sub_assortiment_range sar,
marts.mart_sprav ms
where kind = 1 and docdate between '2024-01-01' and '2024-01-31'
and trim(sar.id) = trim(ap.iditem)
and trim(ms."ID_Контрагенты")=trim(ap.idstore)
and sar.additional_products is true

explain
select
docdate,
ms."Филиал",
revenue - purchase - discount1 - discount2 -discount3 - discount4 -discount5 as profit_net,
sar.additional_products
from
core.assortiment_plain ap
join
marts.sub_assortiment_range sar
on trim(ap.iditem) = trim(sar.id)
join marts.mart_sprav ms
on trim(ap.idstore) = ms."ID_Контрагенты"
where kind = 1
and docdate between '2024-01-01' and '2024-01-31'
and trim(sar.id) = trim(ap.iditem)
and trim(ms."ID_Контрагенты")=trim(ap.idstore)
and sar.additional_products is true