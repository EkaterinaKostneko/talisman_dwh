TRUNCATE TABLE core.act_margin_projects;

INSERT INTO core.act_margin_projects
(
	DocDate,
	PharmacyCode,
	TypeOrder,
	OnlineSale,
	OrderQuantity,
	SallingSum,
	PurchaseSum)
SELECT
	alHead.DocDate AS DocDate,
	PharmacyCode,
	(CASE
		WHEN ((POSITION(';' IN alHead.RecipeNumber)-1)<> 4) AND ((POSITION(';' IN alHead.RecipeNumber)-1)<> 6) AND (SUBSTRING(alHead.RecipeNumber, 1, 1)<> '3') THEN 8
		WHEN ((POSITION(';' IN alHead.RecipeNumber)-1)= 4) OR (((POSITION(';' IN alHead.RecipeNumber)-1)= 8) AND (SUBSTRING(alHead.RecipeNumber, 1, 1)= '3')) THEN 4
		ELSE (POSITION(';' IN alHead.RecipeNumber)-1)
	END) AS TypeOrder,
	(CASE
		WHEN (TRIM(alHead.RecipeNumber)='') THEN 0
		ELSE 1
	END) AS OnLineSale ,
	COUNT(DISTINCT(alHead.ID)) AS OrderQuantity,
	SUM(totalsum)-SUM(DiscountSum) AS SallingSum,
	SUM(checksum_purchase) AS PurchaseSum
FROM
	stg_dwh.act_checkheaders AS alHead
WHERE
	alHead.Status = 1
	AND alHead.WriteOffType is not null
	AND alHead.totalsum>0
	AND alHead.checksum_purchase>0
	GROUP BY
		alHead.DocDate,
		PharmacyCode,
		(CASE
			WHEN ((POSITION(';' IN alHead.RecipeNumber)-1)<> 4) AND ((POSITION(';' IN alHead.RecipeNumber)-1)<> 6) AND (SUBSTRING(alHead.RecipeNumber, 1, 1)<> '3') THEN 8
			WHEN ((POSITION(';' IN alHead.RecipeNumber)-1)= 4) OR (((POSITION(';' IN alHead.RecipeNumber)-1)= 8) AND (SUBSTRING(alHead.RecipeNumber, 1, 1)= '3')) THEN 4
			ELSE (POSITION(';' IN alHead.RecipeNumber)-1)
		END),
		(CASE
		  WHEN (TRIM(alHead.RecipeNumber)='') THEN 0
		  ELSE 1
		END);

truncate table marts.mart_margin_projects_hot;

insert into marts.mart_margin_projects_hot(
    "Дата",
	"Код аптеки проекта",
	"Код контрагента",
	"Номер типа заказа",
	"Тип заказа",
	"Количество",
	"Оборот",
	"Себестоимость",
	"Наценка")
SELECT
        r.docdate AS "Дата",
		r.pharmacycode AS "Код аптеки проекта",
		s."ID_Контрагенты"  AS "Код аптеки",
		r.typeorder AS "Номер типа заказа",
		(CASE
			WHEN r.TypeOrder = 0 THEN 'Без заказов'
			WHEN r.TypeOrder = 8 and r.OnlineSale = 1 THEN 'Твояаптека.рф'
			WHEN r.TypeOrder = 8 and r.OnlineSale = 0 THEN 'Офлайн'
			WHEN r.TypeOrder = 5 THEN 'АСЗ (Доставка)'
			WHEN r.TypeOrder = 4 THEN 'АСЗ (Самовывоз)'
			WHEN r.TypeOrder = 6 THEN 'Семейная-аптека.рф'
			ELSE 'Не определено'
		END) AS TypeOrderName,
		r.orderquantity AS "Количество",
		r.sallingsum AS "Оборот",
		r.purchasesum AS "Себестоимость",
		(r.sallingSum - r.purchaseSum)/ NULLIF(r.purchaseSum, 0) * 100 AS "Наценка"
FROM core.act_margin_projects  r
LEFT JOIN marts.mart_sprav s
ON r.pharmacycode = TRIM(s.МестоХран)
WHERE s."ID_Контрагенты" is not null ;

truncate table marts.mart_total_results_by_store_hot;

insert into marts.mart_total_results_by_store_hot(
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
stg_dwh.act_checkheaders  ch
where
ch.Status = 1 AND
 (ch.ConsumptionType = 1
    OR ch.ConsumptionType = 4
    OR ch.ConsumptionType = 8) AND
 ch.WriteOffFlag = 0
group by DocDate,
PharmacyCode
;

refresh materialized view marts.latest ;