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
	IntQuantity,
	FracQuantity,
	GrossProfit)
SELECT
	ch.DocDate ,
	PharmacyCode,
	Sum(TotalSum)-Sum(DiscountSum)   		as turnover,
	count(distinct ch.DocNumber)            as quanity,
	Sum(TotalSum)                           as TotalSum,
	Sum(BonusesSum)                         as BonusesSum,
	Sum(DiscountSum)                        as DiscountSum,
	Sum(IndividualDiscountSum)              as IndividualDiscountSum,
	Sum(ch.Coupon)                          as Coupon,
	Sum(CheckSum_Purchase)                  as CheckSum_Purchase,
	Sum(CheckSum_Selling)                   as CheckSum_Selling,
	Sum(CheckSum_WithDiscount)              as CheckSum_WithDiscount,
	Sum(ch.IntQuantity)                     as IntQuantity,
	Sum(FracQuantity)                       as FracQuantity,
    Sum(ct.grossprofitsum)                  as GrossProfit
FROM
ods.checkheaders  ch
join
ods.checktables ct
on ch.id = ct.checkid
where
ch.Status = 1 AND
 (ch.ConsumptionType = 1
    OR ch.ConsumptionType = 4
    OR ch.ConsumptionType = 8) AND
 ch.WriteOffFlag = 0
--and ch.DocDate between '01.07.2024' and '01.07.2024'
group by ch.DocDate,
PharmacyCode