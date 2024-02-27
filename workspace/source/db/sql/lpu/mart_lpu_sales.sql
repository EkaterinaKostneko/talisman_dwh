--витрина  Продажи ЛПУ
CREATE OR REPLACE VIEW marts.mart_lpu_sales
AS
SELECT
 TRIM (PARENTEXT) AS ID_Врач,
 TRIM (SP24317) AS ID_Контрагента,
 SP24316 AS НомерРецепта,
 SP24315 AS НомерЧека,
 SP24318 AS Сумма,
 SP24319 AS ДатаЧека,
 SP24320 AS Начисленно,
 SP24335 AS Выплачено,
 TRIM (SC24297.PARENTID) AS ID_Филиал,
 SC208.DESCR AS Менеджер
FROM stg_dwh.sc24313_doctor SC24313
LEFT JOIN stg_dwh.sc24297_receipt_bonus SC24297 ON TRIM(SC24297.ID) = TRIM(SC24313.PARENTEXT)
LEFT JOIN stg_dwh.sc208_staff SC208 ON SC208.ID = SC24297.SP24350
;