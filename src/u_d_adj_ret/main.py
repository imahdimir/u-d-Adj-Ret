"""

    """

from namespace_mahdimir import tse as tse_ns
from namespace_mahdimir import tse_github_data_url as tgdu
from run_py import DefaultDirs
from run_py import rm_cache_dirs
from run_py import run_modules

# namespace
c = tse_ns.Col()

class GDU :
    g = tgdu.GitHubDataUrl()

    adj_price_s = g.adj_price
    tse_wd_s = g.tse_work_days
    adj_ret_t = g.adj_ret

class Dirs :
    dd = DefaultDirs(make_default_dirs = True)

    gd = dd.gd
    t = dd.t

class FPN :
    dyr = Dirs()

    # temp data files
    t0 = dyr.t / 't0.prq'

class ColName :
    frst_d = "FirstDate"
    lst_d = "LastDate"
    lin_fill = "LinearlyFilledAdjClose"

# class instances   %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
gdu = GDU()
dyr = Dirs()
fpn = FPN()
cn = ColName()

def main() :
    pass

    ##
    run_modules()

    ##
    rm_cache_dirs()

##
if __name__ == "__main__" :
    print('\n\n\t\t***** Running main.py *****\n\n')
    main()
    print('\n\n\t\t***** main.py Done! *****\n\n')
