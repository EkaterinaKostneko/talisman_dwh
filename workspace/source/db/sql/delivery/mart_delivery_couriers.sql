truncate table marts.mart_delivery_couriers;

insert into marts.mart_delivery_couriers(
    ID,
	"ФИО курьера")
SELECT
    LTRIM (ID) AS ID,
	DESCR AS "ФИО курьера"
FROM stg_dwh.sc25186_couriers;
