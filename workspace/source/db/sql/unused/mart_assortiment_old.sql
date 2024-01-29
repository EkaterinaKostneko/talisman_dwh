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
with 
r as
(
	select	
		"Дата",
		"ID аптеки",
		"ID товара",
		"ID продукта",
		SUM("Сумма") as "Сумма",
		SUM("СуммаЗакупа") as "СуммаЗакупа",
		SUM("Количество") as "Количество"
	from marts.mart_assortiment_sales_pre
	group by
	"Дата",
	"ID аптеки",
	"ID товара",
	"ID продукта"
	having SUM("Сумма") <> 0
	and SUM("Количество") <> 0
),
m as 
(
select 
		"Дата",
		"ID аптеки",
		"ID товара",
		"ID продукта",
		"Количество",
		"Сумма" AS "ВаловаяВыручка",
		"Сумма"-"СуммаЗакупа" AS "ВаловаяПрибыль",
		"Сумма"/"Количество" AS "ЦенаРозничная",
		("Сумма"-"СуммаЗакупа")/"Количество" AS "ПрибыльСУпаковки",
		(select 
			backmargin
		from marts.mart_backmargin mb
		where mb.product = "ID продукта"
			and mb.docmonth = EXTRACT(MONTH FROM "Дата")
			and mb.docyear = EXTRACT(YEAR FROM "Дата")
		LIMIT 1) as "БМ"
--		("Сумма"-"СуммаЗакупа")/"Количество"+backmargin AS  "ОбщаяПрибыльСУпаковки"		
from r
)
select 
		"Дата",
		"ID аптеки",
		"ID товара",
		"Количество",
		"ВаловаяВыручка",
		"ВаловаяПрибыль",
		"ЦенаРозничная",
		"ПрибыльСУпаковки",
		coalesce("БМ", 0) as "БэкМаржа",
		"ПрибыльСУпаковки"+coalesce("БМ", 0) AS  "ОбщаяПрибыльСУпаковки"	
from m
;