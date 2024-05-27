truncate table marts.mart_delivery_goods;

insert into marts.mart_delivery_goods(
    ID,
	"Дата доставки",
	"Дата оформления",
	"Номер заказа",
	"Номер телефона",
	"Статус",
	"Сумма заказа",
	"Код сайта",
	"Строк заказа",
	"Город",
	"Район_ID",
	"Адрес",
	"Сумма по курьеру",
	"Курьер_ID",
	"Широта",
	"Долгота",
	"Коммент_аптека")
SELECT SC24263.ID,
		sc25134.SP25141 AS "Дата доставки",
		sc24263.SP24278 AS "Дата оформления",
		sc24263.SP24265 AS "Номер заказа",
		sc24263.SP24268 AS "Номер телефона",
		sc24263.SP24269 AS "Статус",
		sc24263.SP24279 AS "Сумма заказа",
		sc24263.SP24705 AS "Код сайта",
		sc24263.SP24706 AS "Строк заказа",
		trim(sc25134.SP25168) AS "Город",
		LTRIM (sc25134.SP25175) AS "Район_ID",
		sc25134.SP25136 AS "Адрес",
		sc25134.SP25144 AS "Сумма по курьеру",
		LTRIM (sc25134.SP25194) AS "Курьер_ID",
		sc25134.SP25138 AS "Широта",
		SC25134.SP25137 AS "Долгота",
		sc25134.SP25174 AS "Коммент_аптека"
FROM stg_dwh.sc25134_delivery sc25134
JOIN ods.sc24263_orders sc24263 ON sc24263.ID = sc25134.PARENTEXT
WHERE trim(sc25134.sp25143)='Выполнен';