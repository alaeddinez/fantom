
select NUM_ART,NUM_ETT,week_of_year,year_of_calendar, sum(QTE_VTE) as QTE_VTE from (
select  NUM_ART ,DAT_VTE ,NUM_ETT,QTE_VTE , extract (year from DAT_VTE) as year_of_calendar,
extract (month from DAT_VTE) as month_of_year,
extract (week from DAT_VTE) as week_of_year
from `dfdp-teradata6y.SalesLmfr.TA001_VTE_REFPETTPJOU`)
where 1 = 1 
and NUM_ETT in (var_store)
and year_of_calendar in (var_year) 
and week_of_year in(var_week)

group by NUM_ART,NUM_ETT,week_of_year,year_of_calendar
