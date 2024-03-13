--витрина Справочник врачей
CREATE OR REPLACE VIEW marts.mart_doctor_list
AS
SELECT
 LTRIM (SC24297.ID) AS ID_Врач,
 LTRIM (SC24297.PARENTID) AS ID_Филиал,
 (CASE
           WHEN SC24297.DESCR like '' THEN 'Пустая строка!'
            ELSE SC24297.DESCR
 END)                                         AS ФИО_Врач,
 SP24299 AS ДатаРождения,
 SP24300 AS Должность,
 SP24301 AS ЛПУ,
 SP24349 AS ДатаДоговора,
 SC208.DESCR AS Менеджер
FROM stg_dwh.sc24297_receipt_bonus  SC24297
LEFT JOIN stg_dwh.sc208_staff  SC208 ON SC208.ID = SC24297.SP24350
WHERE  SC24297.DESCR NOT LIKE '%илиал%'
;