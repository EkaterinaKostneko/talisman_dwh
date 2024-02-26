truncate table marts.mart_total_results_by_store;

insert into marts.mart_total_results_by_store(
	DocDate ,
	PharmacyCode ,
	turnover ,
	quanity,
	TotalSum  ,	
	BonusesSum  ,
	DiscountSum  ,
	IndividualDiscountSum ,
	Coupon ,
	CheckSum_Purchase ,
	CheckSum_Selling ,
	CheckSum_WithDiscount,
	IntQuantity)
SELECT
	DocDate ,
	PharmacyCode,
	Sum(CheckSum_Selling)-Sum(DiscountSum) as turnover,
	count(distinct DocNumber) as quanity,
	Sum(TotalSum) as TotalSum,	
	Sum(BonusesSum) as BonusesSum,
	Sum(DiscountSum) as DiscountSum,
	Sum(IndividualDiscountSum) as IndividualDiscountSum,
	Sum(Coupon) as Coupon,
	Sum(CheckSum_Purchase) as CheckSum_Purchase,
	Sum(CheckSum_Selling) as CheckSum_Selling,
	Sum(CheckSum_WithDiscount) as CheckSum_WithDiscount,
	Sum(IntQuantity) as IntQuantity
FROM 
stg_dwh.checkheaders  ch 
where
ch.Status = 1 AND
 (ch.ConsumptionType = 1
    OR ch.ConsumptionType = 4
    OR ch.ConsumptionType = 8) AND
 ch.WriteOffFlag = 0
--and DocDate between '01.01.2023' and '31.10.2023'
group by DocDate,
PharmacyCode 
