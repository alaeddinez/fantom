select NUM_ART, 1 as flag_inv	 from `dfdp-teradata6y.StocksInStoreLmfr.TF001_SMA_INV`
where 
NUM_ETT in (var_store)
and DAT_INV in ('var_date')
and  NUM_BU = 1 
group by NUM_ART