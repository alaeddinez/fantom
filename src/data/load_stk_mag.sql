select NUM_ART , 0 as flag_stk FROM `dfdp-teradata6y.StocksInStoreLmfr.TA001_SMA_STKMAGGLO`
WHERE num_bu = 1
AND NUM_ETT in (var_store)
and dat_rlvstk in ('var_date')
and QTE_STKOK <= 0
