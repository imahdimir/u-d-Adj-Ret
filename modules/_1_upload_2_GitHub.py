"""

    """

import pandas as pd
from githubdata import clone_overwrite_a_repo__ret_gdr_obj
from githubdata import make_data_fn
from githubdata import upload_2_github
from mirutil.df import save_df_as_prq
from mtok.mtok import ret_local_github_token_filepath

from main import c
from main import fpn
from main import gdu

def ret_data_fn(df) :
    jdate = df[c.jd].max()
    dn = gdu.adj_price_s.split('d-')[1]
    fn = make_data_fn(dn , jdate)
    return fn

def clone_adj_ret() :
    # Get previous adjusted returns data
    return clone_overwrite_a_repo__ret_gdr_obj(gdu.adj_ret_t)

def main() :
    pass

    ##

    # read temp data
    df = pd.read_parquet(fpn.t0)

    ##

    fn = ret_data_fn(df)

    ##

    if ret_local_github_token_filepath() is None :
        print("***Saving Locally on the working directory***")
        save_df_as_prq(df , fn)

        ##
        return

    ##

    gd = clone_adj_ret()

    ##

    upload_2_github(gd , df , fn)

##
if __name__ == "__main__" :
    main()
