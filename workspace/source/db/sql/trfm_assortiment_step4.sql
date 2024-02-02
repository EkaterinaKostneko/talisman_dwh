CREATE OR REPLACE VIEW core.assortiment_plain_step4
AS
SELECT * FROM core.assortiment_plain_step3
WHERE
kind = 1
and quantity<>0;