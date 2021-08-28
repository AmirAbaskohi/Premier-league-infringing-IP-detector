import numpy as np
import pandas as pd
from sklearn import preprocessing

# Define config constants
BUCKET_NAME = "europe-west1-piracy-2e452739-bucket"
SQL_PL_FIXTURES = "SQL_PL_FIXTURES.txt"
SQL_IP_TRAFFICS = "SQL_IP_TRAFFICS.txt"


def read_pl_fixtures():
    """ This function reads premiere league fixtures from bigquery. The query is obtained from the given google storage
    address.

      Args:
      BUCKET_NAME (str): storage bucket name
      SQL_PL_FIXTURES (str): query file location in the bucket

     Returns:
         pl_fixtures (DataFrame): Premier league fixtures for the current season

    """

    # Run the premier league fixtures query and convert the result to dataframe.
    pl_fixtures = pd.read_csv("../../../traffic and fixtures/fixtures.csv")
    pl_fixtures['date'] = pd.to_datetime(pl_fixtures['date'])

    return pl_fixtures


def read_traffic():
    """ This function reads traffic data on the ip level from bigquery. The query is obtained from the given google
     storage address.

      Args:
      BUCKET_NAME (str): storage bucket name
      SQL_IP_TRAFFICS (str): query file location in the bucket
      tomorrow_ds (str): The query date plus one for traffic
      ds (str): The query date for traffic

     Returns:
         ip_traffic (DataFrame): Traffic data for ds

    """
    ip_traffic = pd.read_csv("../../../traffic and fixtures/tmp_traffic_20210412.csv")
    ip_traffic = ip_traffic[ip_traffic['gbps_day'] > 1]
    ip_traffic = ip_traffic.sort_values(['bf_date', 'ip', 'bf_time'], ascending=[True, True, True])

    return ip_traffic


def model_one(ip_traffic, pl_fixtures, match_date):
    """ This function applies the ML model one (an algorithm designed based on the rate of rise of the traffic on ip
    level) for each premier league matches to detect potential infringing ips. The detected ips are separately saved
    for each game within the specified google storage bucket.

     Args:
      ip_traffic (DataFrame): Traffic data for ds
      pl_fixtures (DataFrame): Premier league fixtures for the current season
      match_date (str): the execution date
      BUCKET_NAME (str): storage bucket name
     Returns:
         ip_traffic (DataFrame): Traffic data for ds

    """

    # create a dictionary of game dates and times
    pl_games = {}
    match_dates = list(set(pl_fixtures['date']))
    match_dates = [str(match_date.date()) for match_date in match_dates]
    for md in match_dates:
        pl_games[md] = list(pl_fixtures[pl_fixtures['date'] == md]['ko_time'].unique())

    # Normalizing, Smoothing and Differentiating the traffic per ip
    ip_traffic = ip_traffic.loc[:, ['bf_time', 'ip', 'gbps']]
    ip_pivot = pd.pivot_table(ip_traffic, values='gbps', index=['bf_time'], columns=['ip'], aggfunc=np.sum)

    # filter out non top-talker ips
    ip_pivot = ip_pivot[ip_pivot.columns[ip_pivot.max() > 0.02]]
    ip_pivot = ip_pivot.fillna(0)
    ips = ip_pivot[ip_pivot.columns[:]]
    ips = ips.T

    # minmax normalization
    ips_normalized = preprocessing.minmax_scale(ips.T).T
    ips_normalized = pd.DataFrame(ips_normalized.T)
    ips_normalized.columns = ip_pivot.columns[:]
    ips_normalized.index = ip_pivot.index

    ips_smoothed = ips_normalized.groupby(ips_normalized.index).mean().rolling(window=8).mean().shift(periods=-4)
    ips_d = ips_smoothed.diff(periods=4)

    match_times_idx = []
    for match_time in range(len(pl_games[match_date])):
        s_idx = 12 * int(pl_games[match_date][match_time][0:2]) + int(pl_games[match_date][match_time][3:5]) / 5 - 6
        e_idx = 12 * int(pl_games[match_date][match_time][0:2]) + int(pl_games[match_date][match_time][3:5]) / 5 + 21
        match_times_idx.append(s_idx)
        match_times_idx.append(e_idx)

    # Save identified ips in google storage (bigquery) for each pl game
    count = 0
    blob_list = list()
    for match_time in range(len(pl_games[match_date])):
        ip_pirate_list = (ips_d[int(match_times_idx[count]):int(match_times_idx[count + 1])] > .1).sum()
        ip_pirate_list = ip_pirate_list[ip_pirate_list > 10].sort_values(ascending=False)
        ip_pirate_list = pd.DataFrame(ip_pirate_list.index.get_level_values(0))

        # write the identified ip list in the google storage
        blob_name = 'ips_' + match_date.replace('-','') + "_" + str(pl_games[match_date][match_time]).replace(':','')[0:4] + "_UTC.csv"
        pd.DataFrame(data=ip_pirate_list).to_csv(path_or_buf=blob_name, index=False, encoding="UTF-8")

        # move to the next game for the current match date
        blob_list += [blob_name]
        del ip_pirate_list
        count += 2

    return blob_list
