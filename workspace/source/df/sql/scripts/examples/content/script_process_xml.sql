with t2 as
(
   select
      CAST(value AS XML) as query_column
   from op_data od
   where od.id = '{{AF_OP_DATA_ID}}'  -- идентификатор строки, добавленной текущей операцией
                                      -- od.op_ins_id ({{AF_OP_INS_ID}}) идентификатор текущей операции/задачи
)
select
   val.date,
   val.ask,
   val.volume,
   val.bid,
   val.priceBid,
   val.priceOffer,
   val.blockBid,
   val.blockOffer,
   val.bids,
   val.offers,
   val.period,
   val.periodType
FROM t2,
     XMLTABLE
     (
         '/root/body/dayAheadMarketVolumeList' PASSING query_column
         COLUMNS
              "date" text PATH 'date',
              ask text PATH 'quantityOfAsk',
              volume text PATH 'volume',
              bid text PATH 'quantityOfBid',
              priceBid text PATH 'priceIndependentBid',
              priceOffer text PATH 'priceIndependentOffer',
              blockBid text PATH 'blockBid',
              blockOffer text PATH 'blockOffer',
              bids text PATH 'matchedBids',
              offers text PATH 'matchedOffers',
              "period" text PATH 'period',
              periodType text PATH 'periodType'
      ) val;
