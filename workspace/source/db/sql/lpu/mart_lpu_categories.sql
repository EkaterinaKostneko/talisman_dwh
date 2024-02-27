--витрина Проект ЛПУ в разрезе категорий
CREATE OR REPLACE VIEW marts.mart_lpu_categories
AS
SELECT
    TRIM(SC24313.parentext) AS ID_Врач,
    date_trunc('Month', SP24319) as МесяцЧека,
    count (SP24315) as КоличествоЧеков,
    sum (SP24318) as СуммаЧеков,
     (CASE
    WHEN count (SP24315) >= 51 THEN 'A++'
    WHEN count (SP24315) >=31  THEN 'A+'
    WHEN count (SP24315) >=21  THEN 'A'
    WHEN count (SP24315) >=11  THEN 'B'
    WHEN count (SP24315) >=6  THEN 'C'
    WHEN count (SP24315) >=1  THEN 'D'
    WHEN count (SP24315) <1 THEN 'Не активен'
    END)       AS Категория,
    TRIM (SC24297.PARENTID) AS ID_Филиал
FROM stg_dwh.sc24313_doctor SC24313
LEFT JOIN stg_dwh.sc24297_receipt_bonus  SC24297
ON TRIM(SC24297.ID) = TRIM(SC24313.PARENTEXT)
group by
	TRIM (PARENTEXT),
	date_trunc('Month', SP24319),
	TRIM (SC24297.PARENTID)
;