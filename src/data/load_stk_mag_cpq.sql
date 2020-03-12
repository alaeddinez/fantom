select distinct NUM_ART , 0 as flag_stk_cpq
from (
select A.NUM_ART, QTE_STKOK,Standard_CPQ, (QTE_STKOK / Standard_CPQ) as QTE_STK_CPQ    from ( 
select * FROM `dfdp-teradata6y.StocksInStoreLmfr.TA001_SMA_STKMAGGLO`
WHERE num_bu = 1
AND NUM_ETT in (var_store)
and dat_rlvstk BETWEEN ('var_date_1') and ('var_date_2')
) as A
left join
(select NUM_ART,
case when Standard_CPQ = 0 then 1
else Standard_CPQ
end as Standard_CPQ
from `big-data-dev-lmfr.SUPPLY_PHANTOM.CPQ`) AS B
on A.NUM_ART  = B.NUM_ART
)
where QTE_STK_CPQ <= var_prop
