truncate table marts.mart_delivery_areas;

insert into marts.mart_delivery_areas(
    ID,
	"Район доставки",
	"Город",
    "Филиал",
    "Основная аптека")
SELECT
    LTRIM (sc25152.ID) AS ID,
    trim(sc25152.DESCR) AS "Район доставки",
	trim(t1.DESCR) AS Город,
	trim(t2.DESCR) AS Филиал,
	trim(sc25152.SP25169) AS "Основная аптека"
FROM stg_dwh.sc25152_delivery_area sc25152
LEFT JOIN (SELECT ID, PARENTID, DESCR
		FROM stg_dwh.sc24500_citysite) t1
ON t1.ID = sc25152.SP25154
LEFT JOIN (SELECT ID, PARENTID, DESCR
		FROM stg_dwh.sc24500_citysite) t2
ON t2.ID = t1.PARENTID
WHERE sc25152.DESCR NOT LIKE 'Яя%';