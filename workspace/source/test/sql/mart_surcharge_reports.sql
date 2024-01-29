truncate table marts.mart_surcharge_reports;

insert into marts.mart_surcharge_reports(
    	"Дата",
	"Код аптеки проекта",
	"Код аптеки",
	"Номер типа заказа",
	"Тип заказа",
	"Количество",
	"Оборот",
	"Себестоимость",
	"Наценка")
SELECT
        r.docdate AS "Дата",
		r.pharmacycode AS "Код аптеки проекта",
		s.id,
		r.typeorder AS "Номер типа заказа", 	
		r.typeordername AS "Тип заказа",
		r.orderquantity AS "Количество", 
		r.sallingsum AS "Оборот", 
		r.purchasesum AS "Себестоимость",
		(r.sallingSum - r.purchaseSum)/ r.purchaseSum * 100 AS "Наценка"
FROM ods.surcharge_reports r
left JOIN stg_dwh.sc135_storage s 
on r.pharmacycode = replace(s.SP21449, ' ','') ;