INSERT INTO stg_dwh.Consignments (
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
)
VALUES %s;
