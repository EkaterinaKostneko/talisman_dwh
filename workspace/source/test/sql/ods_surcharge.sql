TRUNCATE TABLE ods.surcharge_reports;

INSERT INTO ods.surcharge_reports
(
	DocDate,
	PharmacyCode,
	TypeOrder,
	TypeOrderName,
	OrderQuantity,
	SallingSum,
	PurchaseSum)
SELECT
	DocDate
	,PharmacyCode
	,TypeOrder
	,(CASE
		WHEN TypeOrder = 0 THEN 'Без заказов'
		WHEN TypeOrder = 8 and OnlineSale = 1 THEN 'Твояаптека.рф'
		WHEN TypeOrder = 8 and OnlineSale = 0 THEN 'Офлайн'
		WHEN TypeOrder = 5 THEN 'АСЗ (Доставка)'
		WHEN TypeOrder = 4 THEN 'АСЗ (Самовывоз)'
		WHEN TypeOrder = 6 THEN 'Семейная-аптека.рф'
		ELSE 'Не определено'
	END) AS TypeOrderName
  	,	OrderQuantity
  	,	SallingSum
  	,	PurchaseSum
FROM
	stg_dwh.surcharge_reports_test;
