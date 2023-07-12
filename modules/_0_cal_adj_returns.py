"""

    """

from pathlib import Path

import pandas as pd
from githubdata import GitHubDataRepo
from mirutil.df import save_df_as_prq
from persiantools.jdatetime import JalaliDateTime

from main import c
from main import cn
from main import fpn
from main import gdu

def keep_relevant_cols(df) :
    c2k = {
            c.ftic   : None ,
            c.d      : None ,
            c.aclose : None ,
            }

    df = df[c2k.keys()]

    return df

def cal_intraday_adj_return(df) :
    df = df.sort_values(by = [c.ftic , c.d])

    df[c.aclose] = df[c.aclose].astype("Int64")

    df[c.arlo] = df.groupby(c.ftic)[c.aclose].pct_change()

    return df

def find_first_and_last_day_for_each_firm(df) :
    df[cn.firstd] = df.groupby(c.ftic)[c.d].transform("min")
    df[cn.lastd] = df.groupby(c.ftic)[c.d].transform("max")
    return df

def keep_only_open_dates_of_tse(df) :
    msk = df[c.is_tse_open].eq(True)

    df = df[msk]

    df = df[[c.d]]

    return df

def make_all_days_for_each_ticker_if_the_market_is_open(dfa , dfb) :
    dfa1 = dfa[[c.ftic , cn.firstd , cn.lastd]].drop_duplicates()

    dfc = pd.merge(dfb , dfa1 , how = 'cross')

    msk = dfc[c.d].le(dfc[cn.lastd])
    msk &= dfc[c.d].ge(dfc[cn.firstd])

    dfc = dfc[msk]

    dfc = dfc[[c.d , c.ftic]]

    dfa = dfa.drop(columns = [cn.firstd , cn.lastd])

    dfd = pd.merge(dfc , dfa , how = 'left')

    dfd = dfd.sort_values(by = [c.ftic , c.d])

    return dfd

def make_is_tic_open_col(df) :
    df[c.is_tic_open] = df[c.aclose].notna()
    return df

def cal_1_workday_return(df) :
    df[c.ar1d] = df.groupby(c.ftic)[c.aclose].pct_change(fill_method = None)
    return df

def cal_1_day_filled_return(df) :
    df[c.aclose] = df[c.aclose].astype(float)

    gpobj = df.groupby(c.ftic , group_keys = False)
    df[cn.lfilld] = gpobj.apply(lambda x : x[[c.aclose]].interpolate())

    gpobj = df.groupby(c.ftic , group_keys = False)
    df[c.ar1dlf] = gpobj[cn.lfilld].pct_change(fill_method = None)

    df = df.drop(columns = [c.aclose , cn.lfilld])

    return df

def make_jd(df) :
    df[c.d] = pd.to_datetime(df[c.d] , format = "%Y-%m-%d")

    df[c.jd] = df[c.d].apply(JalaliDateTime.to_jalali)

    df[c.d] = df[c.d].dt.strftime("%Y-%m-%d")
    df[c.jd] = df[c.jd].apply(lambda x : x.strftime("%Y-%m-%d"))

    return df

def reorder_cols(df) :
    colord = {
            c.ftic        : None ,
            c.d           : None ,
            c.jd          : None ,
            c.is_tic_open : None ,
            c.arlo        : None ,
            c.ar1d        : None ,
            c.ar1dlf      : None ,
            }

    df = df[colord.keys()]

    return df

def main() :
    pass

    ##

    # Get adjusted prices
    gda = GitHubDataRepo(gdu.adj_price_s)
    dfa = gda.read_data()

    ##
    dfa = keep_relevant_cols(dfa)

    ##
    dfa = cal_intraday_adj_return(dfa)

    ##
    dfa = find_first_and_last_day_for_each_firm(dfa)

    ##
    # get tse working days data
    gdsb = GitHubDataRepo(gdu.tse_wd_s)
    dfb = gdsb.read_data()

    ##
    dfb = keep_only_open_dates_of_tse(dfb)

    ##
    df = make_all_days_for_each_ticker_if_the_market_is_open(dfa , dfb)

    ##
    df = make_is_tic_open_col(df)

    ##
    df = cal_1_workday_return(df)

    ##
    df = cal_1_day_filled_return(df)

    ##
    df = make_jd(df)

    ##
    df = reorder_cols(df)

    ##
    save_df_as_prq(df , fpn.t0)

    ##

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
