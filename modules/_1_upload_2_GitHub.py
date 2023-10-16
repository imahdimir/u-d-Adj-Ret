"""

    """

import pandas as pd
from githubdata import clone_overwrite_a_repo__ret_gdr_obj
from mirutil.df import save_df_as_prq
from mtok.mtok import ret_local_github_token_filepath

from main import c
from main import fpn
from main import gdu

def make_data_fn(df) :
    jdate = df[c.jd].max()
    dn = gdu.adj_ret_t.split('d-')[1]
    return "{}_{}.prq".format(dn , jdate)

def clone_adj_ret() :
    # Get previous adjusted returns data
    return clone_overwrite_a_repo__ret_gdr_obj(gdu.adj_ret_t)

def upload(df , fn , gdt) :
    jdate = df[c.jd].max()

    if hasattr(gdt , "data_fp") :
        fp = gdt.data_fp
        fp.unlink()

    fp = gdt.local_path / fn

    save_df_as_prq(df , fp)

    msg = f"Adjusted returns updated until {jdate}"
    msg += f' by {gdu.slf}'

    gdt.commit_and_push(msg)

def main() :
    pass

    ##

    # read temp data
    df = pd.read_parquet(fpn.t0)

    ##

    fn = make_data_fn(df)

    ##

    if ret_local_github_token_filepath() is None :
        print("***Saving Locally on the working directory***")
        save_df_as_prq(df , fn)

        ##
        return

    ##

    gd = clone_adj_ret()

    ##

    upload(df , fn , gd)

##
if __name__ == "__main__" :
    main()
