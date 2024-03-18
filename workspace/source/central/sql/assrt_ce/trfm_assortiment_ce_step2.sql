--добавляем каналы продаж (не используется, добавлено в шаг 1)
CREATE OR REPLACE VIEW core.assortiment_ce_step2
AS SELECT
       r.docdate    as docdate,
       r.iddoc      as iddoc,
--       r.updatedate as updatedate,
       r.idstore    as idstore,
       r.iditem     as iditem,
       r.channel    as
       r.quantity   as quantity,
--       r.return_quantity,
       r.revenue    as revenue,
       r.purchase   as purchase,
       r.discount   as discount
   FROM core.assortiment_ce_step1 as r
   WHERE
   r.Status = 1 AND
    (r.ConsumptionType = 1
       OR r.ConsumptionType = 4
       OR r.ConsumptionType = 8) AND
    r.WriteOffFlag = 0;
