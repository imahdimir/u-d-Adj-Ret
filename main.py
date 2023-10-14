"""

    """

from pathlib import Path

from mirutil.dirr import DefaultDirs
from mirutil.run_modules import clean_cache_dirs
from mirutil.run_modules import run_modules_from_dir_in_order
from namespace_mahdimir import tse as tse_ns
from namespace_mahdimir import tse_github_data_url as tgdu

# namespace
c = tse_ns.Col()

class GDU :
    g = tgdu.GitHubDataUrl()

    adj_ret_t = g.adj_ret

    slf = tgdu.m + 'u-' + adj_ret_t

    adj_price_s = g.adj_price
    tse_wd_s = g.tse_work_days

class Dirs :
    dd = DefaultDirs()

    gd = dd.gd
    t = dd.t

class FPN :
    dyr = Dirs()

    # temp data files
    t0 = dyr.t / 't0.prq'

class ColName :
    frst_d = "FirstDate"
    lst_d = "LastDate"
    lin_fill = "LinearFilledAdjClose"

# class instances   %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
gdu = GDU()
dyr = Dirs()
fpn = FPN()
cn = ColName()

def main() :
    pass

    ##

    run_modules_from_dir_in_order()

    ##

    clean_cache_dirs()

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
