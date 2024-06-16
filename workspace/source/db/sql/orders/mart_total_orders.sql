truncate table marts.mart_total_orders;

insert into marts.mart_total_orders(
    "Дата",
	"Код аптеки",
	"Группа аптек",
	"Источник",
	metrics,
	value
	)
WITH main_table as (select t1."Дата",
             t1."Код аптеки",
	     t1."Группа аптек",
             t1."Источник",
             sum(t1."Сумма заказа") / 1000000 as total_price,
             count(t1."Номер заказа")  / 1000.0 as cnt_orders,
             count(distinct t1."Номер телефона") as unic_users,
             count(distinct t1."Код аптеки") as unic_shops
      from (select mo."Дата",
                   mo."Номер заказа",
                   mo."Код аптеки",
                   mo."Сумма заказа",
                   mo."Номер телефона",
                   case
   				   WHEN mo."Код сайта" in (0, 4, 5) THEN 'АСС'
                   WHEN mo."Код сайта" = 2 THEN 'Семейная-аптека.рф'
                   else 'Твояаптека.рф' end as "Группа аптек",
                   mo."Источник"
            from marts.mart_orders mo
            --left join marts.mart_sprav ms on mo."Код аптеки" = ms."ID_Контрагенты"
          ) t1
group by t1."Дата", t1."Код аптеки", t1."Группа аптек", t1."Источник")

select main_table."Дата", main_table."Код аптеки", main_table."Группа аптек", main_table."Источник", 'total_price' as metrics, main_table.total_price as value from main_table
UNION ALL
select main_table."Дата", main_table."Код аптеки", main_table."Группа аптек", main_table."Источник", 'cnt_orders' as value, main_table.cnt_orders from main_table
UNION ALL
select main_table."Дата", main_table."Код аптеки", main_table."Группа аптек", main_table."Источник", 'unic_users' as value, main_table.unic_users from main_table
UNION ALL
select main_table."Дата", main_table."Код аптеки", main_table."Группа аптек", main_table."Источник", 'unic_shops' as value, main_table.unic_shops from main_table;