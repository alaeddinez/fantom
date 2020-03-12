import pandas as pd
from data import LoadSales, LoadCPQ, LoadInventPeriod, LoadStkMagCPQ
from data import LoadInvent, LoadStkMag, LoadVekiaSuspect
from datetime import datetime
import numpy as np
import os
from data import storage_blob
from utils import *
import datetime



def amplif_coeff(var):
    """[summary]
    
    Arguments:
        var {[type]} -- [description]
    """
    if var < 0:
        var = - (1 + (var*var))
    else:
        var
    return(var)

def string_to_date(str_date, lag):
    """[summary]
    Arguments:
        str_date {[type]} -- [description]
    """
    date_lag = datetime.datetime.strptime(str_date, "%Y-%m-%d")
    date_lag = date_lag - datetime.timedelta(days=lag)
    date_lag = date_lag.strftime("%Y-%m-%d")
    return(date_lag)

def prep_vekia(df, date_execution):
    previous_monday_date = df.columns[3]
    df = df.drop(["POS_ID", "ECART_TYPE"], axis=1)
    df = pd.melt(df, id_vars=['RC_ID'])
    df = df[df.variable == previous_monday_date]
    df.variable = date_execution
    df.rename(columns={'RC_ID': 'NUM_ART'}, inplace=True)
    # estimate daily sales from weekly sales
    df.value = df.value/6
    return(df)

def calcul_score(df_vekia, df_sales, cpq_df):
    # left join the vekia prev with actual values
    merged = df_vekia.merge(df_sales[["NUM_ART", "QTE_VTE"]], on=["NUM_ART"], how='left')
    # fill NA values by 0
    merged.QTE_VTE = merged.QTE_VTE.fillna('0')
    merged.QTE_VTE = merged.QTE_VTE.astype("float")
    # adding cpq information
    merged = merged.merge(cpq_df, on=["NUM_ART"], how='left')
    merged.Standard_CPQ = merged.Standard_CPQ.fillna('1')
    merged.Standard_CPQ = merged.Standard_CPQ.astype("float")
    # calculate the score
    merged["score"] = (merged.value - merged.QTE_VTE) / (merged.Standard_CPQ)
    # using the amplif_coeff when real sales > forecasting
    merged.score = merged['score'].apply(amplif_coeff)
    # always in the intermidiate step the score_cum takes the same value as score
    merged["score_cum"] = merged.score
    return merged

# TODO : unused 
def create_date_range(start_date, end_date):
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end- start).days)]
    list_date = list()
    for date in date_generated:
        li = date.strftime("%Y-%m-%d")
        list_date.append(li)
    list_date = np.unique(list_date)
    return(list_date)



def score_cum_day(real_sales, prev_sales, cpq_table, inv_table, stk_table, date, store):
    #load the necessary data
    prev_sales = prep_vekia(prev_sales, date)
    # using prev_vekia_function  to calculate the intermidiate score
    data_score_days = calcul_score(prev_sales, real_sales, cpq_table)
    #sort by ref/day
    data_score_days = data_score_days.sort_values(by=['NUM_ART', 'variable'])
    #############################################################
    #          push alerts
    #############################################################
    # initiate flag_alerte to 1
    data_score_days["flag_alerte"] = 1
    # join with the inventory data
    merged = data_score_days.merge(inv_table, on=["NUM_ART"], how='left')
    # join with the stock data
    merged = merged.merge(stk_table, on=["NUM_ART"], how='left')
    # ne pas pousser l'alerte lorsque le stock mag est inf ou égal à 0
    # flag_stk not null = qté_stock <= 0
    merged.flag_alerte[merged.flag_stk.notnull()] = 0
    
    # ###############################################################
    # ############## processing j -1 ################################
    # ###############################################################
    date_execution_lag = string_to_date(str_date=date, lag=1)
    try:
        day_before = pd.read_csv("../output/score_" +
                                 ''.join(e for e in date_execution_lag if e.isalnum())
                                 + ".csv", sep=";")
    except FileNotFoundError:
        print("the corresponding file doesnt exist ==> initiating")
        # empty table in case file not found
        day_before = pd.DataFrame(columns=merged.columns.values)
    
    day_before = day_before[["NUM_ART", "score_cum", "flag_inv"]]
    day_before.rename(columns={'score_cum': 'score_cum_before'}, inplace=True)
    day_before.rename(columns={'flag_inv': 'flag_inv_before'}, inplace=True)
    # print merged
    merged = merged.merge(day_before, on=["NUM_ART"], how='left')
    # replace null with 0 in case of new sku per example
    merged.score_cum_before = merged.score_cum_before.fillna(0)
    merged["score_cum"] = merged["score_cum"] + merged["score_cum_before"]
    # inv not null = pas d'inventaire
    # le score cumulé tombe à 0
    merged.score_cum[merged.flag_inv.notnull()] = 0
    merged.flag_alerte[merged.score_cum <= 0] = 0
    # ###############################################################
    # ############## inventaire 3 semaines   ########################
    # ###############################################################
    date_3weeks = string_to_date(str_date=date, lag=22)
    inv_3weeks = LoadInventPeriod('inv_period', store=store,
                                  date_1=date_3weeks,
                                  date_2=date).dataframe
    merged = merged.merge(inv_3weeks, on=["NUM_ART"], how='left')
    #merged.rename(columns={'flag_inv_period': 'flag_inv_3weeks'}, inplace=True)
    # dont push alert if inventory has value during 3 last weeks                           
    merged.flag_alerte[merged.flag_inv_period.notnull()] = 0
    # score_cum can't be negative
    merged.score_cum[merged.score_cum < 0] = 0
    return(merged)