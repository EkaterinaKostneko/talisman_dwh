DROP TABLE IF EXISTS marts.sub_assortiment_range;

CREATE TABLE marts.sub_assortiment_range (
        id varchar(9) ,
        parentid varchar(9) ,
        code varchar(6) ,
        descr varchar(100) ,
        isfolder int4 ,
        ismark bool ,
        verstamp int4 ,
        product_group_id varchar(9),
        product_group_descr varchar(100) ,
        product_category_id varchar(9),
        product_category_descr varchar(100) ,
        mnn_directory_id varchar(9),
        mnn_directory_descr varchar(100) ,
        price_segment varchar(100) ,
        economic_groups_id varchar(9),
        economic_groups_descr varchar(100) ,
        additional_products bool,
        marking_type varchar(9)
);

CREATE OR REPLACE VIEW  marts.mart_range
AS
SELECT
code 					"ID товара",
descr 					"Наименование товара",
product_group_descr 	"Товарная группа",
product_category_descr 	"Товарная категория",
mnn_directory_descr 	"МНН",
price_segment 			"Ценовой сегмент",
economic_groups_descr 	"Экономическая группа",
additional_products 	"Дополнительный ассортимент",
marking_type 			"Вид маркировки"
FROM marts.sub_assortiment_range sar
