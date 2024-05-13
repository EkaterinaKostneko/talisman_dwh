SELECT
	ID ,
	DateTime ,
	ProductID ,
	PharmCode ,
	Code ,
	Code1C ,
	DocID ,
	DocType ,
	DocNumber ,
	DocDate ,
	MfrCode ,
	MfrName ,
	Series ,
	ShelfLife,
	Price ,
	Quantity ,
	Fasovka ,
	StorageQty ,
	StoragePlace ,
	MarkingFlag ,
	UpdateDate ,
	VSDQuantity ,
	Name ,
	Reserve
FROM
	[Central].[dbo].[Consignments]
--WHERE DocDate >=start_date
;