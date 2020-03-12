import pandas as pd
from data import LoadSales, LoadCPQ, LoadInventPeriod, LoadStkMagCPQ
from data import LoadInvent, LoadStkMag, LoadVekiaSuspect
from datetime import datetime
import numpy as np
import os
from data import storage_blob
from utils import *
import datetime
# TODO : intiate the loop with the first csv file (in this case 01/01/2020)
# loading CPQ
# TODO : cpq table is pointing on a temporal table(access is forbidden on origin table)
CPQ_df = LoadCPQ('cpq').dataframe
stores = ['3']
date_execution = "2020-02-18"
#for store in stores:
store = '143'
print(store)
# ##### real sales j ###########
SALES_df = LoadSales('day_sales', option_source="bq", store=store,
                        date=date_execution).dataframe
# ##### vekia prev j ###########
data = storage_blob(bucket='big-data-dev-supply-sages',
                    blob='EXTRACTION_PV_' + store + '_'
                    + ''.join(e for e in date_execution if e.isalnum())
                    + '.csv').select_bucket(sep=";")
# #### inventory data j ########
inv_data = LoadInvent('inventory', date=date_execution,
                        store=store).dataframe
# #### stk mag data j ##########
stk_data = LoadInvent('stk_mag', store=store,
                        date=date_execution).dataframe

# TODO : dans la fonction score_cum_day on fait appel à d'autres donnees 
# TODO : voir charger les  donnnes dans le main ?
current_data = score_cum_day(real_sales=SALES_df, prev_sales=data,
                                cpq_table=CPQ_df, inv_table=inv_data,
                                stk_table=stk_data, date=date_execution,
                                store=store)

#TODO : changer le chemin pour l'indus : bucket ?
current_data.to_csv("../output/score_" +
                        ''.join(e for e in date_execution if e.isalnum()) +
                        ".csv",
                        sep=";")




#TODO : sauvgarder le resultat final dans une autre table
# ce resultat va contenir les TOP 10 de chaque rayon avec flag alerte => 130 réf par magasin