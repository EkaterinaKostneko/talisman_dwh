INSERT INTO stg_dwh.checktables_inc (
	ID,
	DateTime,
	CheckID,
	TovarCode1C,
	TovarName,
	DocType,
	DocNumber,
	DocDate,
	ProCode,
	ProName,
	ProAsString,
	Series,
	ShelfLife,
	Price,
	PurchasePrice,
	SellingPrice,
	DiscountPrice,
	IntQuantity,
	FracQuantity,
	Sum,
	Discount,
	Akciya,
	Coupon,
	DiscByDiscCard,
	DiscOfSite,
	DiscByRecipe,
	DiscByBonusCard,
	DiscByAkciya,
	VSDQuantity,
	EconomicGroupCode
	)
VALUES %s;
