"""

    """

from pathlib import Path

import pandas as pd
from githubdata import GitHubDataRepo
from mirutil.ns import update_ns_module
from persiantools.jdatetime import JalaliDateTime

update_ns_module()
import ns

gdu = ns.GDU()
c = ns.Col()

sfp = Path('temp.prq')

class ColName :
    lastd = "LastDate"
    firstd = "FirstDate"
    lfilld = "LinearFilled"

cn = ColName()

def main() :
    pass

    ##

    # Get adjusted prices
    gdsa = GitHubDataRepo(gdu.srca)
    dfa = gdsa.read_data()

    ##
    c2k = {
            c.ftic   : None ,
            c.d      : None ,
            c.aclose : None ,
            }

    dfa = dfa[c2k.keys()]

    ##
    dfa = dfa.sort_values(by = [c.ftic , c.d])

    ##
    dfa[c.aclose] = dfa[c.aclose].astype("Int64")

    ##
    dfa[c.arlo] = dfa.groupby(c.ftic)[c.aclose].pct_change()

    ##
    dfa[cn.firstd] = dfa.groupby(c.ftic)[c.d].transform("min")
    dfa[cn.lastd] = dfa.groupby(c.ftic)[c.d].transform("max")

    ##
    gdsb = GitHubDataRepo(gdu.srcb)
    dfb = gdsb.read_data()

    ##
    msk = dfb[c.is_tse_open].eq(True)

    dfb = dfb[msk]

    ##
    dfb = dfb[[c.d]]

    ##
    dfa1 = dfa[[c.ftic , cn.firstd , cn.lastd]].drop_duplicates()

    ##
    dfc = pd.merge(dfb , dfa1 , how = 'cross')

    ##
    msk = dfc[c.d].le(dfc[cn.lastd])
    msk &= dfc[c.d].ge(dfc[cn.firstd])

    dfc = dfc[msk]

    ##
    dfc = dfc[[c.d , c.ftic]]

    ##
    dfa = dfa.drop(columns = [cn.firstd , cn.lastd])

    ##
    dfd = pd.merge(dfc , dfa , how = 'left')

    ##
    dfd = dfd.sort_values(by = [c.ftic , c.d])

    ##
    dfd[c.ar1d] = dfd.groupby(c.ftic)[c.aclose].pct_change(fill_method = None)

    ##
    dfd[c.is_tic_open] = dfd[c.aclose].notna()

    ##
    dfd[c.aclose] = dfd[c.aclose].astype(float)

    ##
    gpobj = dfd.groupby(c.ftic , group_keys = False)
    dfd[cn.lfilld] = gpobj.apply(lambda x : x[[c.aclose]].interpolate())

    ##
    gpobj = dfd.groupby(c.ftic , group_keys = False)
    dfd[c.ar1dlf] = gpobj[cn.lfilld].pct_change(fill_method = None)

    ##
    dfd = dfd.drop(columns = [c.aclose , cn.lfilld])

    ##
    dfd[c.d] = pd.to_datetime(dfd[c.d] , format = "%Y-%m-%d")

    ##
    dfd[c.jd] = dfd[c.d].apply(JalaliDateTime.to_jalali)

    ##
    dfd[c.d] = dfd[c.d].dt.strftime("%Y-%m-%d")
    dfd[c.jd] = dfd[c.jd].apply(lambda x : x.strftime("%Y-%m-%d"))

    ##
    colord = {
            c.ftic        : None ,
            c.d           : None ,
            c.jd          : None ,
            c.is_tic_open : None ,
            c.arlo        : None ,
            c.ar1d        : None ,
            c.ar1dlf      : None ,
            }

    dfd = dfd[colord.keys()]

    ##
    dfd.to_parquet(sfp , index = False)

    ##
    gdsa.rmdir()
    gdsb.rmdir()

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
