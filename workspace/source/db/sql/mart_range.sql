DROP VIEW IF EXISTS marts.mart_range_pretty cascade;

CREATE OR REPLACE VIEW marts.mart_range_pretty
AS
SELECT
	sr.CODE "ID товара",
	REPLACE(sr.DESCR, 'яя', '') "Наименование товара",
	REPLACE(spg.DESCR, 'яя', '') "Товарная группа",
	REPLACE(spc.DESCR, 'яя', '') "Товарная категория",
	REPLACE(smd.DESCR, 'яя', '') "МНН",
	sr.SP25230 "ЦеновойСегмент",
	REPLACE(seg.DESCR, 'яя', '') "Экономическая группа",
	(CASE
	WHEN spg.DESCR like '%Лекарственные средства и БАД%' THEN false
	WHEN spg.DESCR like '%Медицинские изделия%' THEN false
	WHEN spg.DESCR like '%Прочие%' THEN false
	ELSE true
	END) 						AS "ДопАссортимент",
	SP26079
from stg_dwh.sc156_range sr
left join stg_dwh.sc25892_product_groups spg
on sr.SP26010 = spg.ID
left join stg_dwh.sc25097_product_categories spc
on sr.SP25096 = spc.ID
left join stg_dwh.sc23072_mnn_directory smd
on sr.SP23076 = smd.ID
left join stg_dwh.sc25148_economic_groups seg
on sr.SP25150 = seg.ID;