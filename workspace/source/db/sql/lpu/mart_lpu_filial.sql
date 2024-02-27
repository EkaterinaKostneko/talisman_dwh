--витрина Справочник врачей
CREATE OR REPLACE VIEW marts.mart_filial
AS
SELECT
 LTRIM(ID) AS ID_Филиал,
 DESCR AS Филиал
FROM stg_dwh.sc24297_receipt_bonus SC24297
WHERE TRIM (PARENTID) = '0';
