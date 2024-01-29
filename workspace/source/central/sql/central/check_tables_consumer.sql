INSERT INTO stg_dwh.check_tables(
	ID,
	CheckID,
	DocDate,
	PharmacyCode,
	RecipeNumber,
	PurchasePrice,
	IntQuantity,
	FracQuantity,
	Price,
	SellingPrice,
	DiscountPrice,
	DiscByBonusCard,
	Status,
	WriteOffType)
VALUES %s;
