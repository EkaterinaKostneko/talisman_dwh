TRUNCATE TABLE ods._1sjourn;

INSERT INTO ods._1sjourn
(	
	row_id,
	idjournal,
	iddoc,
	date_time_iddoc,
	docdate)
SELECT 
	row_id,
	idjournal,
	iddoc,
	date_time_iddoc,
	CAST (SUBSTRING(_1SJOURN.DATE_TIME_IDDOC, 1, 8) as date) AS DocDate
from stg_dwh._1sjourn
;
