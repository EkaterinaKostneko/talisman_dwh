TRUNCATE TABLE core.assortiment_ce_plain;


INSERT INTO core.assortiment_ce_plain (
	IDHeader            ,
	IDTable             ,
	DocDate             ,
	IDdoc               ,
	UpdateDate          ,
	IDstore             ,
	IDitem              ,
	CheckID             ,
    Status              ,
    ConsumptionType     ,
    WriteOffFlag        ,
	quantity            ,
	revenue             ,
	discount            ,
	purchase            ,
    Price               ,
    PurchasePrice       ,
    SellingPrice        ,
    DiscountPrice       ,
    IntQuantity         ,
    FracQuantity        ,
    Sum                 ,
    Akciya              ,
    Coupon              ,
    DiscByDiscCard      ,
    DiscOfSite          ,
    DiscByRecipe        ,
    DiscByBonusCard     ,
    DiscByAkciya        ,
    VSDQuantity         ,
    EconomicGroupCode   ,
    RecipeNumber        ,
    TypeRaw             ,
    TypeOrder           ,
    OnLineSale
)
SELECT
    Doc.ID                  as IDHeader,
    DocTabl.ID              as IDTable,
	Doc.DocDate             as DocDate,
	Doc.DocNumber     		as IDdoc,
	Doc.DateTime            as UpdateDate,
	TRIM(PharmacyCode)      as IDstore,
	TRIM(TovarCode1C)       as IDitem,
	DocTabl.CheckID         as CheckID,
    Doc.Status              as Status,
    Doc.ConsumptionType     as ConsumptionType,
    Doc.WriteOffFlag        as WriteOffFlag,
	DocTabl.IntQuantity             as quantity,
	DocTabl.Sum                     as revenue ,
	DocTabl.Discount                as discount ,
    DocTabl.PurchasePrice*DocTabl.IntQuantity
                                    as purchase ,
    DocTabl.Price                   as Price,
    DocTabl.PurchasePrice           as PurchasePrice,
    DocTabl.SellingPrice            as SellingPrice,
    DocTabl.DiscountPrice           as DiscountPrice,
    DocTabl.IntQuantity             as IntQuantity,
    DocTabl.FracQuantity            as FracQuantity,
    DocTabl.Sum                     as Sum,
    DocTabl.Akciya                  as Akciya,
    DocTabl.Coupon                  as Coupon,
    DocTabl.DiscByDiscCard          as DiscByDiscCard,
    DocTabl.DiscOfSite              as DiscOfSite,
    DocTabl.DiscByRecipe            as DiscByRecipe,
    DocTabl.DiscByBonusCard         as DiscByBonusCard,
    DocTabl.DiscByAkciya            as DiscByAkciya,
    DocTabl.VSDQuantity             as VSDQuantity,
    DocTabl.EconomicGroupCode    	as EconomicGroupCode,
    Doc.RecipeNumber ,
    POSITION(';' in Doc.RecipeNumber) - 1 as TypeRaw,
	 CASE
	    WHEN ((POSITION(';' in Doc.RecipeNumber) - 1) <> 4) AND ((POSITION(';' in  Doc.RecipeNumber) - 1) <> 6) AND (SUBSTRING(Doc.RecipeNumber FROM 1 FOR 1) <> '3') THEN 8
	    WHEN ((POSITION(';' in  Doc.RecipeNumber) - 1) = 4) OR (((POSITION(';' in  Doc.RecipeNumber) - 1) = 8) AND (SUBSTRING(Doc.RecipeNumber FROM 1 FOR 1) = '3')) THEN 4
	    ELSE (POSITION(';' in  Doc.RecipeNumber) - 1)
	  END AS TypeOrder,
	(CASE
		WHEN (TRIM(Doc.RecipeNumber)='') THEN 0
		ELSE 1
	END) AS OnLineSale
FROM 	ods.checkheaders    AS Doc
LEFT JOIN ods.checktables   AS DocTabl
	ON Doc.ID=DocTabl.CheckID
where Doc.DocDate between '2024-02-01' and '2024-02-01'