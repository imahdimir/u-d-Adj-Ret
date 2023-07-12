"""

    """

from pathlib import Path

import pandas as pd
from githubdata import GitHubDataRepo
from mirutil.df import save_df_as_prq

from main import c
from main import fpn
from main import gdu

def upload(df , gdt) :
    jdate = df[c.jd].max()

    if hasattr(gdt , "data_fp") :
        fp = gdt.data_fp
        fp.unlink()

    fp = gdt.local_path / f"{jdate}.prq"

    save_df_as_prq(df , fp)

    msg = f"Adjusted returns updated until {jdate}"
    msg += f' by {gdu.slf}'

    gdt.commit_and_push(msg)

def compare_2_cols_with_nan(df , col1 , col2) :
    df = df[[col1 , col2]]
    df = df.astype('float64')

    chk = df[col1].eq(df[col2])

    msk1 = df[col1].isna() & df[col2].isna()

    msk = chk.eq(False)
    chk[msk] = msk1[msk]

    return chk.eq(True)

def make_merged_df_for_checking(df_old , df_new) :
    df = pd.merge(df_old , df_new , how = "inner" , on = [c.ftic , c.d , c.jd])
    return df

def check_ticker_open_status_not_changed(df) :
    chk = df[c.is_tic_open + "_x"].eq(df[c.is_tic_open + "_y"])

    assert chk.all() , "Ticker open status changed"

def check_adjret_cols(df) :
    cols_2_ck = [c.arlo , c.ar1d]

    for col in cols_2_ck :
        chk = compare_2_cols_with_nan(df , col + "_x" , col + "_y")

        assert chk.all() , f"{col} changed"

def check_ar1dlf_col(df) :
    msk = df[c.ar1dlf + "_x"].notna()

    df1 = df[msk]

    chk = compare_2_cols_with_nan(df1 , c.ar1dlf + "_x" , c.ar1dlf + "_y")
    assert chk.all() , f"{c.ar1dlf} changed"

def main() :
    pass

    ##

    # Get previous adjusted returns data
    gdt = GitHubDataRepo(gdu.adj_ret_t)
    gdt.clone_overwrite()

    ##
    # if it is the first time, upload the data else continue checking
    if hasattr(gdt , "data_fp") :
        dft = gdt.read_data()
    else :
        dft = pd.read_parquet(fpn.t0)
        upload(dft , gdt)

        ##
        return

    ##
    dfn = pd.read_parquet(fpn.t0)

    ##
    dfc = make_merged_df_for_checking(dft , dfn)

    ##
    check_ticker_open_status_not_changed(dfc)

    ##
    check_adjret_cols(dfc)

    ##
    check_ar1dlf_col(dfc)

    ##
    upload(dfn , gdt)

    ##

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')

##


if False :
    pass

    ##
    def test() :
        pass

        ##
        fp = '/Users/mahdi/Dropbox/GitHub/u-d-Adjusted-Returns/d-Adjusted-Returns/data.prq'

        df = pd.read_parquet(fp)

        ##

        ##
