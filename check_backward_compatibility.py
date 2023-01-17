"""

    """

from pathlib import Path

import pandas as pd
from githubdata import GitHubDataRepo
from mirutil.ns import rm_ns_module

from cal_adj_returns import sfp
import ns

gdu = ns.GDU()
c = ns.Col()

class ColName :
    pass

cn = ColName()

def upload(df , gdt) :
    if hasattr(gdt , "data_fp") :
        fp = gdt.data_fp
    else :
        fp = gdt.local_path / "data.prq"

    df.to_parquet(fp , index = False)

    jdate = df[c.jd].max()
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

def main() :
    pass

    ##

    # Get previous adjusted returns data

    gdt = GitHubDataRepo(gdu.trg)
    gdt.clone_overwrite()

    ##
    if hasattr(gdt , "data_fp") :
        dft = gdt.read_data()
    else :
        dft = pd.read_parquet(sfp)
        upload(dft , gdt)
        gdt.rmdir()

        ##
        return

    ##
    dfn = pd.read_parquet(sfp)

    ##
    dfc = pd.merge(dft , dfn , how = "inner" , on = [c.ftic , c.d , c.jd])

    ##
    chk = dfc[c.is_tic_open + "_x"].eq(dfc[c.is_tic_open + "_y"])

    assert chk.all() , "Ticker open status changed"

    ##
    cols_2_ck = [c.arlo , c.ar1d]

    for col in cols_2_ck :
        chk = compare_2_cols_with_nan(dfc , col + "_x" , col + "_y")

        assert chk.all() , f"{col} changed"

    ##
    msk = dfc[c.ar1dlf + "_x"].notna()

    df1 = dfc[msk]

    chk = compare_2_cols_with_nan(df1 , c.ar1dlf + "_x" , c.ar1dlf + "_y")
    assert chk.all() , f"{c.ar1dlf} changed"

    ##
    upload(dfn , gdt)

    ##
    rm_ns_module()

    ##
    gdt.rmdir()
    sfp.unlink()

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
