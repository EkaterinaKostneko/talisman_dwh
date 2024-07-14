DROP TABLE IF EXISTS stg_dwh.checktables_inc ;

CREATE TABLE stg_dwh.checktables_inc (
	ID              int        ,
	DateTime        timestamp  ,
	CheckID         integer     ,
	TovarCode1C     varchar(30) ,
	TovarName       varchar(80) ,
	DocType         varchar(20) ,
	DocNumber       varchar(10) ,
	DocDate         date ,
	ProCode         int ,
	ProName         varchar(80) ,
	ProAsString     varchar(40) ,
	Series          varchar(30) ,
	ShelfLife       date ,
	Price           decimal(12,2) ,
	PurchasePrice   decimal(12,2) ,
	SellingPrice    decimal(12,2) ,
	DiscountPrice   decimal(12,2) ,
	IntQuantity     decimal(12,2) ,
	FracQuantity    int ,
	Sum             decimal(12,2) ,
	Discount        decimal(12,2) ,
	Akciya          decimal(12,2) ,
	Coupon          decimal(12,2) ,
	DiscByDiscCard  decimal(12,2) ,
	DiscOfSite      decimal(12,2) ,
	DiscByRecipe    decimal(12,2) ,
	DiscByBonusCard decimal(12,2) ,
	DiscByAkciya    decimal(12,2) ,
	VSDQuantity     decimal(12,4) ,
	EconomicGroupCode int
);

ALTER TABLE stg_dwh.checktables_inc
	ADD COLUMN GrossProfitSum decimal(12,2) ,
	ADD COLUMN SalesChannel int,
	ADD COLUMN OrderNumber varchar(100),
	ADD COLUMN DiscountsAsStr varchar(100),
	ADD COLUMN Sk_Akciya decimal(12,4),
	ADD COLUMN Sk_Zakaz decimal(12,4) ,
	ADD COLUMN Sk_Recept decimal(12,4),
	ADD COLUMN Sk_Bonus decimal(12,4),
	ADD COLUMN Sk_Okr decimal(12,4)
	;

