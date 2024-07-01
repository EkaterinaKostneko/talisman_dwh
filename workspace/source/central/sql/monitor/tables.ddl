DROP TABLE IF EXISTS stg_dwh.mon_CheckHeaders ;

CREATE TABLE stg_dwh.mon_CheckHeaders (
 Count      int,
 DT         date,
 DocDate    date,
 CheckSum_Purchase      numeric(14, 2),
 CheckSum_Selling       numeric(14, 2),
 CheckSum_WithDiscount  numeric(14, 2)
);

