INSERT INTO stg_dwh.surcharge_reports
(
	DocDate,
	PharmacyCode,
	TypeOrder,
	OnlineSale,
	OrderQuantity,
	SallingSum,
	PurchaseSum
	)
VALUES %s;
