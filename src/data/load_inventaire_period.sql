select NUM_ART, sum(1) as flag_inv_period	 from `dfdp-teradata6y.StocksInStoreLmfr.TF001_SMA_INV`
where 
NUM_ETT in (var_store)
and DAT_INV between  ('var_date_1') and ('var_date_2')
and  NUM_BU = 1 
group by NUM_ART