TRUNCATE TABLE marts.mart_sprav;

INSERT INTO marts.mart_sprav
(
    "ID_Хранение",
	"ID_Контрагенты",
	"ID_Подразделение",
	"МестоХран",
	"Аптека",
	"Город",
	"Филиал",
	"Фирма",
	"Формат",
	"Круглосуточные",
	"Широта",
	"Долгота",
	"ОбщаяПлощадь",
	"ТорговаяПлощадь",
	"КоличествоКасс",
	"ДатаКоличУчета",
	"ТипЦен",
	"ЦеноваяКатегория",
	"ГруппаВыкладки",
	"ДниЗапаса",
	"ДниЗаказа",
	"КатегорияАптеки",
	"КатегорияБонус",
	"КатегорияМарк",
  	"Аналитик",
	"УРС",
	"ГруппаАптекПоТО"
)
with
	t1 (ID, PARENTID, DESCR) AS
		(SELECT id, parentid , descr
            FROM stg_dwh.sc135_storage
            WHERE descr LIKE '%филиал%' or descr like '%ф-л%'),
	t2 (ID, PARENTID, DESCR) AS
		(SELECT id, parentid , descr
		FROM stg_dwh.sc135_storage
		WHERE descr LIKE 'Аптеки%')
select trim(SC135.ID) AS ID_Хранение,
		trim(SC133.ID) AS ID_Контрагенты,
		trim(SC135.SP22303) AS ID_Подразделение,
		trim(SC135.SP21449) AS МестоХран,
		SC135.DESCR AS Аптека,
	    trim(REPLACE (t2.DESCR, 'Аптеки ', '')) AS Город,
	    trim(REPLACE (t1.DESCR, 'Аптеки ', '')) AS Филиал,
	    trim(SC133.SP21322) AS Фирма,
	    trim(SC133.SP24507) AS Формат,
	    SC133.SP24510 AS Круглосуточные,
	    SC133.SP24505 AS Широта,
		SC133.SP24506 AS Долгота,
		SC135.SP24866 AS ОбщаяПлощадь,
		SC135.SP24867 AS ТорговаяПлощадь,
		SC135.SP25941 AS КоличествоКасс,
		SC133.SP21253::date AS ДатаКоличУчета,
		trim(SC135.SP22655) AS ТипЦен,
		SC135.SP25151 AS ЦеноваяКатегория,
		SC135.SP26007 AS ГруппаВыкладки,
		SC135.SP21514 AS ДниЗапаса,
		SC135.SP21515 AS ДниЗаказа,
		SC135.SP22476 AS КатегорияАптеки,
		SC135.SP24708 AS КатегорияБонус,
		SC135.SP22872 AS КатегорияМарк,
  		TRIM (SC135.SP25889) AS Аналитик,
		TRIM (SC135.SP25890) AS УРС,
		TRIM(t3.value) as ГруппаАптекПоТО
		FROM stg_dwh.sc135_storage SC135
INNER JOIN t2 ON t2.ID = SC135.PARENTID
LEFT JOIN t1 ON t1.ID = t2.PARENTID
LEFT JOIN stg_dwh.sc133_contragent SC133 ON SC133.SP21194 = SC135.ID
LEFT JOIN LATERAL (
	select distinct on (1) 
		objid, 
		value,
		date
	from stg_dwh._1sconst s 
	where id  = 26218
	and s.objid = sc135.ID
	order by 1, date desc 
) t3 ON true;