"""

    """

import shutil
from pathlib import Path

from githubdata import default_containing_dir
from mirutil.run_modules import run_modules_from_dir_in_order
from namespace_mahdimir import tse as tse_ns
from namespace_mahdimir import tse_github_data_url as tgdu

# namespace     %%%%%%%%%%%%%%%
c = tse_ns.Col()

class GDU :
    g = tgdu.GitHubDataUrl()

    adj_ret_t = g.adj_ret

    slf = tgdu.m + 'u-' + adj_ret_t

    adj_price_s = g.adj_price
    tse_wd_s = g.tse_work_days

class Dirs :
    md = Path('modules/')
    md.mkdir(exist_ok = True)

    gd = default_containing_dir

    td = Path('temp_data/')
    td.mkdir(exist_ok = True)

class FPN :
    dyr = Dirs()
    td = dyr.td

    # temp data files
    t0 = td / 't0.prq'

class ColName :
    lastd = "LastDate"
    firstd = "FirstDate"
    lfilld = "LinearFilled"

# class instances   %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
gdu = GDU()
dyr = Dirs()
fpn = FPN()
cn = ColName()

def clean_cache() :
    print('cleaning cache ...')

    dyrs = {
            dyr.gd : None ,
            dyr.td : None ,
            }

    for di in dyrs.keys() :
        shutil.rmtree(di , ignore_errors = True)

def main() :
    pass

    ##
    run_modules_from_dir_in_order(dyr.md)

    ##
    clean_cache()

    ##

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
