
select  NUM_ART ,DAT_VTE ,NUM_ETT,SUM (QTE_VTE) QTE_VTE
from `dfdp-teradata6y.SalesLmfr.TA001_VTE_REFPETTPJOU`
where 1 = 1 
and NUM_ETT in (var_store)
and DAT_VTE in ('var_date')
group by NUM_ART,NUM_ETT,DAT_VTE
