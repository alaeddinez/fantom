select productId as NUM_ART ,valueStockSuspect  from `lmfr-ddp-ods-dev.suspect_stock_api.tf_suspect_stock_api`
where 
1=1
and 
storeId = var_store
and
dateExtract between 'var_date' and 'var_date'
order by  valueStockSuspect desc