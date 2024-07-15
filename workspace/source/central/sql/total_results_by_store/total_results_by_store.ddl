DROP TABLE IF EXISTS marts.mart_total_results_by_store ;

CREATE TABLE marts.mart_total_results_by_store (
	docdate date NULL,
	pharmacycode varchar(10) NULL,
	turnover numeric(12, 4) NULL,
	quanity int4 NULL,
	totalsum numeric(12, 4) NULL,
	bonusessum numeric(12, 4) NULL,
	discountsum numeric(12, 4) NULL,
	individualdiscountsum numeric(12, 4) NULL,
	coupon numeric(12, 4) NULL,
	checksum_purchase numeric(12, 4) NULL,
	checksum_selling numeric(12, 4) NULL,
	checksum_withdiscount numeric(12, 4) NULL,
	intquantity numeric(12, 4) NULL,
    FracQuantity int4,
    GrossProfit numeric(12,2)
);