TRUNCATE TABLE core.act_margin_projects;

INSERT INTO core.act_margin_projects
(
     Count,
     DT,
     DocDate,
     CheckSum_Purchase,
     CheckSum_Selling,
     CheckSum_WithDiscount
SELECT
