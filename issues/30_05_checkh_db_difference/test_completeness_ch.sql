without indexes
select
sum(purchaseprice)
from
ods.checktables c
where
docnumber = '143'
and docdate = '2022-06-14'

3m 22s

select
c.docnumber,
c.docdate,
c.series,
sum(ct.purchaseprice)
from
ods.checktables ct
join
stg_dwh.consignments c
on
	c.docnumber = ct.docnumber
	and c.docdate = ct.docdate
	and c.series = ct.series
group by
	c.docnumber,
c.docdate,
c.series