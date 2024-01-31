CREATE OR REPLACE VIEW core.assortiment_plain_step3
AS
SELECT * FROM core.assortiment_plain_add_return
UNION ALL
SELECT * FROM core.assortiment_plain_step2;