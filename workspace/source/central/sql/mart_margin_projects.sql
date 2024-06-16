truncate table marts.mart_margin_projects;

insert into marts.mart_margin_projects(
    "Дата",
	"Код аптеки проекта",
	"Код контрагента",
	"Номер типа заказа",
	"Тип заказа",
	"Количество",
	"Оборот",
	"Себестоимость",
	"Наценка")
SELECT
        r.docdate AS "Дата",
		r.pharmacycode AS "Код аптеки проекта",
		s."ID_Контрагенты"  AS "Код аптеки",
		r.typeorder AS "Номер типа заказа", 	
		(CASE
			WHEN r.TypeOrder = 0 THEN 'Без заказов'
			WHEN r.TypeOrder = 8 and r.OnlineSale = 1 THEN 'Твояаптека.рф'
			WHEN r.TypeOrder = 8 and r.OnlineSale = 0 THEN 'Офлайн'
			WHEN r.TypeOrder = 5 THEN 'АСЗ (Доставка)'
			WHEN r.TypeOrder = 4 THEN 'АСЗ (Самовывоз)'
			WHEN r.TypeOrder = 6 THEN 'Семейная-аптека.рф'
			ELSE 'Не определено'
		END) AS TypeOrderName,t
		r.orderquantity AS "Количество", 
		r.sallingsum AS "Оборот", 
		r.purchasesum AS "Себестоимость",
		(r.sallingSum - r.purchaseSum)/ NULLIF(r.purchaseSum, 0) * 100 AS "Наценка"
FROM ods.margin_projects  r
LEFT JOIN marts.mart_sprav s 
ON r.pharmacycode = TRIM(s.МестоХран)
WHERE s."ID_Контрагенты" is not null ;