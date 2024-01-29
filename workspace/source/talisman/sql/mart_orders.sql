truncate table marts.mart_orders;

insert into marts.mart_orders(
    "Дата",
	"Номер заказа",
	"Номер телефона",
	"Код аптеки",
	"Оплачен",
	"Доставка",
	"Статус",
	"Промокод",
	"Сумма заказа",
	"Оплачено бонусами",
	"Код сайта",
	"Строк заказа",
	"Источник")
SELECT
        SP24278::date AS "Дата",
		SP24265 AS "Номер заказа",
		SP24268 AS "Номер телефона",
		REPLACE(SP24270, ' ', '') AS "Код аптеки",
		SP24273 AS "Оплачен",
		SP25121 AS "Доставка",
		SP24269 AS "Статус",
		SP24277 AS "Промокод",
		SP24279 AS "Сумма заказа",
		SP25100 AS "Оплачено бонусами",
		SP24705 AS "Код сайта",
		SP24706 AS "Строк заказа",
		case 
			when trim(SP26087) in ('order','v22_order','order_delivery', 'v22_order_delivery') then 'order'
			when trim(SP26087) in ('mobile_order', 'mobile1_order_delive', 'mobile2_order', 'mobile2_order_delive') then 'mobile_order'
			else null
		end as "Источник"
FROM ods.sc24263_orders
        WHERE SP24273 = 1 AND SP24272 = 0;