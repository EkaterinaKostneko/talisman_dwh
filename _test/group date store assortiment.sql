select
docdate,
idstore,
sum(quantity) quantity ,
sum(return_quantity) return_quantity ,
--sum(move_quantity) move_quantity ,
sum(revenue) revenue ,
sum(purchase) purchase ,
sum(revenue)-sum(discount) rebate,
sum(revenue) - sum(discount) - sum(purchase) as profit,
sum(discount) as discount
from
core.assortiment_final
where docdate = '2023-11-20' and idstore like '%H7%'
group by
docdate,
idstore