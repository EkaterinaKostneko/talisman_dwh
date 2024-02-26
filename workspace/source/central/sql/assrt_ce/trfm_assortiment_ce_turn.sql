--переключаем на основное ядро
CREATE OR REPLACE VIEW core.assortiment_ce_in
AS SELECT
        *
   FROM core.assortiment_ce_plain;