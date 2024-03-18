ALTER TABLE core.assortiment_ce_plain RENAME TO assortiment_ce_plain_last;

CREATE TABLE core.assortiment_ce_plain (
	IDHeader            int4,
	IDTable             int4,
	DocDate             date,
	IDdoc               varchar(9),
	lineno              smallint,
	UpdateDate          timestamp,
	IDstore             varchar(30),
	IDitem              varchar(30),
	CheckId             int,
	Status              int2,
	ConsumptionType     int2,
	WriteOffFlag        int2,
	quantity            int4,
	return_quantity     int4,
	move_quantity       int4,
	revenue             numeric(12, 2),
	purchase            numeric(12, 2),
    discount            numeric(12, 2),
    manufacture         numeric(12, 2),
    kind                int2,
    Price               decimal(12,2) ,
    PurchasePrice       decimal(12,2) ,
    SellingPrice        decimal(12,2) ,
    DiscountPrice       decimal(12,2) ,
    IntQuantity         decimal(12,2) ,
    FracQuantity        int ,
    Sum                 decimal(12,2) ,
    Akciya              decimal(12,2) ,
    Coupon              decimal(12,2) ,
    DiscByDiscCard      decimal(12,2) ,
    DiscOfSite          decimal(12,2) ,
    DiscByRecipe        decimal(12,2) ,
    DiscByBonusCard     decimal(12,2) ,
    DiscByAkciya        decimal(12,2) ,
    VSDQuantity         decimal(12,4) ,
    EconomicGroupCode   int ,
	RecipeNumber        varchar(20),
    TypeRaw             int2,
    TypeOrder           int2,
    TypeOrderName       varchar(20),
    OnlineSale          int2
);

--создать секцию переименования индексов, чтобы избежать конфликта имен
--конец секции переименования индексов

CREATE INDEX  assortiment_ce_plain_docdate_idx ON core.assortiment_ce_plain USING btree (docdate);
CREATE INDEX  assortiment_ce_plain_iddoc_idx ON core.assortiment_ce_plain USING btree (iddoc);
CREATE INDEX  assortiment_ce_plain_iditem_idx ON core.assortiment_ce_plain USING btree (iditem);
CREATE INDEX  assortiment_ce_plain_idstore_idx ON core.assortiment_ce_plain USING btree (idstore);
CREATE INDEX  assortiment_ce_plain_updatedate_idx ON core.assortiment_ce_plain USING btree (updatedate);
CREATE INDEX  assortiment_ce_plain_quantity_idx ON core.assortiment_ce_plain (quantity)
    WHERE quantity = 0;
CREATE INDEX  assortiment_ce_plain_channel_idx ON core.assortiment_ce_plain USING btree (typeorder);


