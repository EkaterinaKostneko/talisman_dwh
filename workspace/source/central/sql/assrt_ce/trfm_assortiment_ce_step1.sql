DROP VIEW IF EXISTS core.assortiment_ce_step1 CASCADE;

--отбираем нужные поля и фильтруем
CREATE OR REPLACE VIEW core.assortiment_ce_step1
AS SELECT
       r.docdate    as docdate,
       r.iddoc      as iddoc,
--       r.updatedate as updatedate,
       r.idstore    as idstore,
       r.iditem     as iditem,
       r.quantity   as quantity,
--       r.return_quantity,
       r.revenue    as revenue,
       r.purchase   as purchase,
       r.discount  -
            r.discbybonuscard -
            r.discofsite -
            r.discbydisccard -
            r.discbyrecipe -
            r.discbyakciya
                    as discount,
       r.recipenumber
                    as recipenumber,
       r.onlinesale as onlinesale,
       r.typeorder  as typeorder,
       (CASE
            WHEN TypeOrder = 0 THEN 'Без заказов'
            WHEN TypeOrder = 8 and OnlineSale = 1 THEN 'Твояаптека.рф'
            WHEN TypeOrder = 8 and OnlineSale = 0 THEN 'Офлайн'
            WHEN TypeOrder = 5 THEN 'АСЗ (Доставка)'
            WHEN TypeOrder = 4 THEN 'АСЗ (Самовывоз)'
            WHEN TypeOrder = 6 THEN 'Семейная-аптека.рф'
            ELSE 'Не определено'
        END)        as TypeOrderName
   FROM core.assortiment_ce_in as r
   WHERE
   r.Status = 1 AND
    (r.ConsumptionType = 1
       OR r.ConsumptionType = 4
       OR r.ConsumptionType = 8) AND
    r.WriteOffFlag = 0;