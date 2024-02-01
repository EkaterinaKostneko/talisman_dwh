TRUNCATE TABLE core.sub_assortiment_range;

INSERT INTO core.sub_assortiment_range
(
id,
parentid,
code,
descr,
isfolder,
ismark,
verstamp,
product_group_id,
product_group_descr,
product_category_id,
product_category_descr,
mnn_directory_id,
mnn_directory_descr,
price_segment,
economic_groups_id,
economic_groups_descr,
additional_products,
marking_type
)
SELECT
    sr.id,
	sr.parentid,
	sr.code,
	REPLACE(sr.DESCR, 'яя', '')     descr,
	sr.isfolder,
	sr.ismark,
	sr.verstamp,
	spg.id    						product_category_id,
	REPLACE(spg.DESCR, 'яя', '')    product_category_descr,
	spc.id                          product_category_id,
	REPLACE(spc.DESCR, 'яя', '')    product_category_descr,
	smd.id                          mnn_directory_id,
	REPLACE(smd.DESCR, 'яя', '')    mnn_directory_descr,
	sr.SP25230                      price_segment,
	seg.id                          economic_groups_id,
	REPLACE(seg.DESCR, 'яя', '')    economic_groups_descr,
	(CASE
	WHEN spg.DESCR like '%Лекарственные средства и БАД%' THEN false
	WHEN spg.DESCR like '%Медицинские изделия%' THEN false
	WHEN spg.DESCR like '%Прочие%' THEN false
	ELSE true
	END) 						    additional_products,
	SP26079                         marking_type
from stg_dwh.sc156_range sr
left join stg_dwh.sc25892_product_groups spg
on sr.SP26010 = spg.ID
left join stg_dwh.sc25097_product_categories spc
on sr.SP25096 = spc.ID
left join stg_dwh.sc23072_mnn_directory smd
on sr.SP23076 = smd.ID
left join stg_dwh.sc25148_economic_groups seg
on sr.SP25150 = seg.ID;