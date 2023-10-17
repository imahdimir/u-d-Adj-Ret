"""

    """

import pandas as pd
from githubdata import get_data_wo_double_clone
from mirutil.df import save_df_as_prq

from main import c
from main import cn
from main import fpn
from main import gdu

def get_adj_prices() :
    return get_data_wo_double_clone(gdu.adj_price_s)

def keep_relevant_cols(df) :
    c2k = {
            c.ftic   : None ,
            c.d      : None ,
            c.aclose : None ,
            }

    df = df[c2k.keys()]

    return df

def keep_only_large_enough_prices(df) :
    """
    because of rounding to int, small numbers are not reliable
    """
    msk = df[c.aclose].astype(int).gt(10)
    df = df[msk]
    return df

def get_tse_work_days() :
    # get tse working days data
    return get_data_wo_double_clone(gdu.tse_wd_s)

def keep_only_open_dates_of_tse(df) :
    msk = df[c.is_tse_open].eq(True)
    df = df[msk]
    return df

def find_first_and_last_day_for_each_firm(df) :
    df[cn.frst_d] = df.groupby(c.ftic)[c.d].transform("min")
    df[cn.lst_d] = df.groupby(c.ftic)[c.d].transform("max")
    return df

def make_all_days_for_each_ticker_if_mkt_open(df_price , df_market_open) :
    # keep ticker and first and last day of each ticker
    df1 = df_price[[c.ftic , cn.frst_d , cn.lst_d]].drop_duplicates()

    # make all days for each ticker if market open, cartesian product
    df = pd.merge(df_market_open , df1 , how = 'cross')

    # keep only days between first and last day of each ticker
    msk = df[c.d].le(df[cn.lst_d])
    msk &= df[c.d].ge(df[cn.frst_d])

    df = df[msk]

    # add price data to each day
    df = pd.merge(df , df_price , how = 'left')

    # sort on date
    df = df.sort_values(by = [c.d])

    return df

def assert_no_duplicate_rows(df) :
    """ no dup rows on (ticker, date) pair """
    msk = df.duplicated(subset = [c.ftic , c.d] , keep = False)
    df1 = df[msk]
    assert df1.empty , "There are duplicated rows"

def gen_is_tic_open_col(df) :
    df[c.is_tic_open] = df[c.aclose].notna()
    return df

def gen_linearly_filled_adj_close(df) :
    df[c.aclose] = df[c.aclose].astype(float)

    # groupby by ticker and fill na linearly
    gps = df.groupby(c.ftic , group_keys = False)
    df[cn.lin_fill] = gps.apply(lambda x : x[[c.aclose]].interpolate())

    return df

def gen_1_workday_filled_return(df) :
    gps = df.groupby(c.ftic , group_keys = False)
    df[c.ar1dlf] = gps[cn.lin_fill].pct_change()
    return df

def assert_only_first_dates_is_nan_in_ret_then_drop_those(df) :
    msk = df[c.d].eq(df[cn.frst_d])
    df1 = df[msk]
    assert df1[c.ar1dlf].isna().all() , "First date is not nan"
    df = df[~msk]
    assert df[c.ar1dlf].notna().all() , "Not first date is nan"
    return df

def assert_no_na(df) :
    assert df.notna().all(axis = None) , "There is nan in the dataframe"

def reorder_cols(df) :
    colord = {
            c.ftic        : None ,
            c.d           : None ,
            c.jd          : None ,
            c.is_tic_open : None ,
            c.wd          : None ,
            c.ar1dlf      : None ,
            }

    df = df[colord.keys()]

    return df

def main() :
    pass

    ##

    dfp = get_adj_prices()
    dfp = keep_relevant_cols(dfp)
    dfp = keep_only_large_enough_prices(dfp)

    ##

    dfw = get_tse_work_days()
    dfw = keep_only_open_dates_of_tse(dfw)

    ##

    dfp = find_first_and_last_day_for_each_firm(dfp)
    df = make_all_days_for_each_ticker_if_mkt_open(dfp , dfw)

    ##

    assert_no_duplicate_rows(df)

    ##

    df = gen_is_tic_open_col(df)

    ##

    df = gen_linearly_filled_adj_close(df)
    df = gen_1_workday_filled_return(df)

    ##

    df = assert_only_first_dates_is_nan_in_ret_then_drop_those(df)

    ##

    df = reorder_cols(df)

    ##

    assert_no_na(df)

    ##

    save_df_as_prq(df , fpn.t0)

##
if __name__ == "__main__" :
    main()
