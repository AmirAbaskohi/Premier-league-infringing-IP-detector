import argparse
import datetime as dt
import os
import warnings
from string import Template

from dateutil.parser import parse

from common.exceptions.custom_exceptions import (BigQueryError,
                                                 CloudStorageError)
from common.functions.gcp_utils import (bq_df_to_table, bq_query_to_df,
                                        bq_query_to_query_job,
                                        construct_bq_client,
                                        construct_storage_client,
                                        cs_get_object_as_list, cs_list_folder,
                                        cs_move_object)
from common.functions.gen_utils import read_config, read_query

########################################################################
# Data processing functions

def get_ips(storage_client, src_bucket, src_folder, input_file_name):
    """ Gets a list of IPs from a csv file in GCP Cloud Storage

    This functions scans all the files in the defined bucket (and
    optionally folder) on GCP's Cloud Storage. If there's a matching
    file, it returns that file's relative path in the bucket and the
    list of IPs that the file contains. If there are no files in the
    defined location, there is no matching file, or there are multiple
    matching files, it raises an error.

    Args:
        storage_client (client obj): client for GCP Cloud Storage API
        src_bucket (str): bucket name
        src_folder (str): folder name (use empty string for no folder)
        input_file_name (str): file name

    Raises:
        CloudStorageError: If no files in defined location
        CloudStorageError: If no matching file
        CloudStorageError: If multiple matching files

    Returns:
        str: relative path of IP file in the bucket
        list: list of IPs from the file
    """

    # List contents of defined bucket or folder within bucket
    blob_list = cs_list_folder(storage_client, src_bucket, src_folder)

    # If there is nothing in the bucket or folder, raise error
    if len(blob_list) == 0:
        raise CloudStorageError(
            f'No IP files in:\nBucket: {src_bucket}\nFolder: {src_folder}')

    # Iterate through list of files and store those that match the
    # input_file_name arg
    blob_to_process = []
    for blob_name in blob_list:
        blob_basename = os.path.basename(blob_name)
        if blob_basename == input_file_name:
            blob_to_process.append(blob_name)

    # Raise error if no matching files or multiple matching files.
    # Otherwise, get list of IPs from file.
    if len(blob_to_process) == 0:
        raise CloudStorageError(f'Could not find {input_file_name}')
    elif len(blob_to_process) > 1:
        raise CloudStorageError(
            f'Multiple matching files for {input_file_name}')
    else:
        ip_list = cs_get_object_as_list(
            storage_client,
            src_bucket,
            blob_to_process[0])

    return blob_to_process[0], ip_list


def get_ip_traffic(bq_client, query_file_name, ip_tuple, ko_timestamp):
    """ Queries BQ using query with substituted IPs and analysis period

    This function gets the detected ips traffic query from file and
    substitutes into it the given ip_tuple arg and analysis period
    relative to the given ko_timestamp arg. The query is then run on
    BigQuery and a Pandas Dataframe is returned.

    The query floors timestamps to 5 min intervals i.e. 00:00:00 ->
    00:04:59 = 00:00:00, summing the gigabits transferred over the 5 min
    interval.

    Args:
        bq_client (client obj): client for GCP BigQuery API
        query_file_name (str): file name of the query
        ip_tuple (tuple): the IPs traffic data should be returned for
        ko_timestamp (datetime): timestamp of kick off in UTC

    Returns:
        DataFrame: Traffic data for IPs within analysis period
    """

    # Get query and store string
    query_str = read_query(query_file_name)

    # Substitute in values
    query_template = Template(query_str)
    query = query_template.substitute(
        analysis_start = ko_timestamp + dt.timedelta(minutes=-90),
        analysis_end   = ko_timestamp + dt.timedelta(minutes=110),
        ip_tuple       = ip_tuple)

    # Run query
    input_ip_traffic_df =  bq_query_to_df(bq_client, query)

    return input_ip_traffic_df


def check_ips_and_warn(src_bucket, blob_name, ip_list, input_ip_traffic_df):
    """ Checks for missing IPs in the IP traffic DF, logs missing IPs

    Args:
        src_bucket (str): bucket name
        blob_name (str): file relative path in bucket
        ip_list (list): original list of IPs to check against
        input_ip_traffic_df (DataFrame): new data to check IPs for
    """

    missing_ips = set(ip_list) - set(input_ip_traffic_df['ip'])
    if len(missing_ips) > 0:
        warnings.warn(
        f'{blob_name} in {src_bucket} no traffic data for IPs: {missing_ips}')

    return


def get_season_and_gw(bq_client, query_file_name, ko_timestamp):
    """ Gets the season and game week for the kick off timestamp

    This function gets the season and game week query from file and
    substitutes into it the ko_timestamp arg. The query is then run on
    BigQuery and the season and game week matching the ko timestamp is
    returned.

    Args:
        bq_client (client obj): client for GCP BigQuery API
        query_file_name (str): file name of the query
        ko_timestamp (datetime): timestamp of kick off in UTC

    Raises:
        BigQueryError: if multiple game weeks match ko_timestamp

    Returns:
        str: the season matching the ko timestamp
        int: the game week matching the ko timestamp
    """

    # Get query and store string
    query_str = read_query(query_file_name)

    # Substitute in values
    query_template = Template(query_str)
    query = query_template.substitute(ko_timestamp = ko_timestamp)

    # Run query
    query_job = bq_query_to_query_job(bq_client, query)

    # Convert QueryJob to list
    query_list = [[row['season'], row['game_week']] for row in query_job]

    # If the query result list is 0 (no matching game week) or more then
    # 1 (multiple matching game weeks) raise an error
    if len(query_list) == 0:
        raise BigQueryError(f'No matching game week for {ko_timestamp}')
    elif len(query_list) > 1:
        raise BigQueryError(f'Multiple matching game weeks for {ko_timestamp}')
    else:
        return query_list[0][0], int(query_list[0][1])


def construct_output_df(ko_timestamp, season, gw, input_ip_traffic_df):
    """ Cleans and processes the input DF, and constructs the output DF

    This function cleans and processes the input DataFrame. It
    determines the piracy traffic by calculating the background noise as
    the mean traffic for each IP from 1.5hrs before the game to 0.5hrs
    before the game and subtracting this from total traffic. It returns
    a DataFrame with total and piracy traffic in gigabits and gbps for
    each IP in 5 min intervals across the game. It includes ASN info,
    the kick off timestamp, game week, and season in the output.

    The query, that is used to create input_ip_traffic_df, floors
    timestamps to 5 min intervals i.e. 00:00:00 -> 00:04:59 = 00:00:00,
    summing the gigabits transferred over the 5 min interval.

    Args:
        ko_timestamp (datetime): timestamp of kick off in UTC
        input_ip_traffic_df (DataFrame): traffic data

    Returns:
        DataFrame: output IP traffic DataFrame to be loaded into BQ
    """

    #### Create the base DataFrame
    # This DataFrame contains the IP, asn info, and kick off timestamp
    # for the IP traffic data to be joined to. The chain sorts by asn
    # with na values first and then drops duplicates keeping the last
    # values. This creates a distinct DataFrame of IPs with asn info.
    # Creating this df and rejoining the traffic data to it is required
    # due to issues in the bigflow data where, for one IP, some records
    # have asn data and some do not.
    base_df = (input_ip_traffic_df
        [['ip', 'asn', 'as_name', 'analyse', 'vpn', 'vpn_name']].
        sort_values('asn', na_position='first').
        drop_duplicates(subset=['ip'], keep='last').
        convert_dtypes().
        set_index('ip'))
    base_df['season'] = season
    base_df['game_week'] = gw
    base_df['ko_timestamp'] = ko_timestamp

    #### Cleaning
    # Similar to the above step, this is required as there are usually
    # duplicate traffic records for an IP within one 5 min timestamp.
    # This is due to issues in the bigflow data where, for the same IP,
    # some record have asn data and some do not. This results in the
    # detected_ips_traffic_query returning one IP record with asn data
    # and one IP record without asn data within a single 5 min
    # timestamp.
    clean_ip_traffic_df = (input_ip_traffic_df
        [['timestamp', 'ip', 'gigabits']].
        groupby(['ip', 'timestamp']).
        sum().
        reset_index())

    #### Calculate noise
    # Noise is calculated as the mean traffic for each IP from 1.5hrs
    # before the game to 0.5hrs before the game.
    # Calculate noise for each IP
    noise_df = (clean_ip_traffic_df
        [(clean_ip_traffic_df['timestamp'] >=
            ko_timestamp + dt.timedelta(minutes=-90)) &
        (clean_ip_traffic_df['timestamp'] <
            ko_timestamp + dt.timedelta(minutes=-30))]
        [['ip', 'gigabits']].
        groupby('ip').
        # Calculates the mean gigabits transferred per 5 mins for each
        # IP over the 1 hour. There are 12 5min intervals in 1 hour.
        # This method is used instead of a standard mean as IPs might be
        # missing records for some of the 12 timestamps.
        agg(lambda x: x.sum()/12).
        rename({'gigabits': 'noise_gigabits'}, axis=1))

    # Join noise value for each IP to traffic data on IP
    processed_ip_traffic_df = (clean_ip_traffic_df
        [(clean_ip_traffic_df["timestamp"] >= ko_timestamp) &
        (clean_ip_traffic_df["timestamp"] <
            ko_timestamp + dt.timedelta(minutes=110))].
        merge(noise_df, how='left', on='ip').
        # If no noise data, fill with 0s
        fillna(value={'noise_gigabits': 0}))

    #### Calculate piracy gigabits
    # Subtract noise gigabits from total gigabits to get piracy gigabits
    processed_ip_traffic_df['piracy_gigabits'] = (
        (processed_ip_traffic_df['gigabits'] -
            processed_ip_traffic_df['noise_gigabits']).
        clip(lower = 0)) # This stops any 'negative' piracy

    #### Calculate gbps
    # For each IP calculate gbps for each 5min timestamp
    processed_ip_traffic_df['gbps'] = (processed_ip_traffic_df
        ['gigabits'].
        div(300)) # 5 mins

    #### Calculate piracy gbps
    # For each IP calculate piracy gbps for each 5min timestamp
    processed_ip_traffic_df['piracy_gbps'] = (processed_ip_traffic_df
        ['piracy_gigabits'].
        div(300)) # 5 mins

    #### Construct output DataFrame
    # Join processed IP traffic to the base DataFrame
    output_ip_traffic_df = base_df.merge(
        processed_ip_traffic_df[['timestamp', 'ip', 'gigabits',
        'piracy_gigabits', 'gbps', 'piracy_gbps']], on='ip')
    output_ip_traffic_df.reset_index()

    return output_ip_traffic_df


def bq_delete_existing_rows(
        bq_client, query_file_name, ko_timestamp, table_id):
    """ Deletes rows in table with matching kick off timestamp

    Args:
        bq_client (client obj): client for GCP BigQuery API
        query_file_name (str): file name of the query
        ko_timestamp (datetime): timestamp of kick off in UTC
        table_id (str): id of the table in BQ
    """

    # Get query and store string
    query_str = read_query(query_file_name)

    # Substitute in values
    query_template = Template(query_str)
    query = query_template.substitute(
        table_id       = table_id,
        ko_timestamp   = ko_timestamp)

    # Run query
    bq_query_to_df(bq_client, query)

    return

########################################################################
# Input processing

def parse_and_check_tz(file_name):
    """ Parse timestamp and ensure in UTC

    Args:
        file_name (str): the file name to be processed

    Raises:
        ValueError: if file name has no parsable TZ or UTC offset

    Returns:
        datetime: timestamp in UTC
    """

    # Parse timestamp
    ko_timestamp = parse(file_name, fuzzy=True)

    # If no TZ info raise error
    if ko_timestamp.tzinfo is None:
        raise ValueError(
            'Input date must contain a parsable timezone or UTC offset.')

    # Set to UTC
    ko_timestamp = ko_timestamp.astimezone(dt.timezone.utc)

    return ko_timestamp


def get_file_name_w_ko(dev, **context):
    """ Get a list of file names and corresponding ko timestamps

    Args:
        dev (bool): set to True if not parsing file names from xcom

    Returns:
        list: file names and corresponding ko timestamps
    """

    # In dev mode, parse file list from the argument input
    if dev == True:
        # Set up argument input
        parser = argparse.ArgumentParser()
        parser.add_argument('file_list',
            nargs='+',
            type=str,
            help='File names must have ko timestamp with TZ or UTC offset')

        # Parse input
        args = parser.parse_args()
        file_list = args.file_list

    # In production mode, parse file list from xcom
    else:
        task_instance = context['task_instance']
        file_list = task_instance.xcom_pull(
            task_ids='ml_model_one', key='Blob List')

    # Get list of ko_timestamps
    file_name_w_ko_list = [
        (file_name, parse_and_check_tz(file_name)) for file_name in file_list]

    return file_name_w_ko_list

########################################################################
# Main function

def main(dev=False, **context):
    """ Function for program execution

    Args:
        dev (bool, optional): set True if developing, defaults False

    Raises:
        FileNotFoundError: if can't locate delete_rows_by_ko_query
    """

    # For log
    print(f'Dev mode {dev}')

    # Get user inputs
    file_name_w_ko_list = get_file_name_w_ko(dev, **context)
    config = read_config('pl_uk_ip_piracy_report_traffic.ini')

    # For log
    print('\nCLOUD STORAGE CONFIG')
    print('project_id: {}'.format(config['CLOUD_STORAGE']['project_id']))
    print('src_bucket: {}'.format(config['CLOUD_STORAGE']['src_bucket']))
    print('src_folder: {}'.format(config['CLOUD_STORAGE']['src_folder']))
    print('dst_bucket: {}'.format(config['CLOUD_STORAGE']['dst_bucket']))
    print('dst_folder: {}'.format(config['CLOUD_STORAGE']['dst_folder']))
    print('\nBIGQUERY CONFIG')
    print('project_id: {}'.format(config['BIGQUERY']['project_id']))
    print('detected_ips_traffic_query: {}'.format(
        config['BIGQUERY']['detected_ips_traffic_query']))
    print('delete_rows_by_ko_query: {}'.format(
        config['BIGQUERY']['delete_rows_by_ko_query']))
    print('season_and_game_week_query: {}'.format(
        config['BIGQUERY']['season_and_game_week_query']))
    print('match_ip_traffic_table_id: {}\n'.format(
        config['BIGQUERY']['match_ip_traffic_table_id']))

    # If running in dev mode, get local credentials
    if dev == True:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = (
            config['DEV_CONFIG']['GOOGLE_APPLICATION_CREDENTIALS'])

        # For log
        print('Using dev Google application credentials')

    # For log
    print('Constructing clients')

    # Construct client
    bq_client = construct_bq_client(config['CLOUD_STORAGE']['project_id'])
    storage_client = construct_storage_client(
        config['BIGQUERY']['project_id'])

    # For log
    print('\nBeginning processing loop\n')

    # Iterate through files
    for input_file_name, ko_timestamp in file_name_w_ko_list:

        # For log
        print('------------------------------------------------------------')
        print(f'Processing {input_file_name}')
        print('Extracting IPs from file')

        # Get IP file name and list of IPs to be processed
        blob_name, ip_list = get_ips(
            storage_client,
            config['CLOUD_STORAGE']['src_bucket'],
            config['CLOUD_STORAGE']['src_folder'],
            input_file_name)

        # For log
        print('Getting traffic data')

        # Get traffic data for IPs for match period
        input_ip_traffic_df = get_ip_traffic(
            bq_client,
            config['BIGQUERY']['detected_ips_traffic_query'],
            tuple(ip_list),
            ko_timestamp)

        # Check for missing IPs in input_ip_traffic_df against original list
        check_ips_and_warn(
            config['CLOUD_STORAGE']['src_bucket'],
            blob_name,
            ip_list,
            input_ip_traffic_df)

        # For log
        print('Matching ko timestamp to season and game week')

        # Get the season and game week for the ko timestamp
        season, gw = get_season_and_gw(
            bq_client,
            config['BIGQUERY']['season_and_game_week_query'],
            ko_timestamp)

        # For log
        print('Constructing output DataFrame')

        # Construct output DataFrame
        output_ip_traffic_df = construct_output_df(
            ko_timestamp,
            season,
            gw,
            input_ip_traffic_df)

        # For log
        print('Deleting any existing data in table for ko timestamp')

        # For ko_timestamp, if data already exists in table, delete
        # existing rows. BQ raises an error if the table doesn't
        # exist, hence try/except.
        try:
            bq_delete_existing_rows(
                bq_client,
                config['BIGQUERY']['delete_rows_by_ko_query'],
                ko_timestamp,
                config['BIGQUERY']['match_ip_traffic_table_id'])
        except FileNotFoundError as error:
            raise error
        except:
            pass

        # For log
        print('Loading DataFrame into table')

        # Create BQ table of output DataFrame
        bq_df_to_table(
            bq_client,
            output_ip_traffic_df,
            config['BIGQUERY']['match_ip_traffic_table_id'])

        # For log
        print('Moving IP file')

        # Move IP file to processed location
        cs_move_object(
            storage_client,
            config['CLOUD_STORAGE']['src_bucket'],
            blob_name,
            config['CLOUD_STORAGE']['dst_bucket'],
            config['CLOUD_STORAGE']['dst_folder'])

        # For log
        print('Complete')

    print('------------------------------------------------------------')
    print('Loop complete')

    return


if __name__ == "__main__":
    main(dev=True)