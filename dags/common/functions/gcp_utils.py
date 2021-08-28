import os

from google.cloud import bigquery, storage

########################################################################
# Google BigQuery functions

def construct_bq_client(project_id):
    """ Constructs client for GCP BigQuery API

    Args:
        project_id (string): name of project

    Returns:
        client obj: client for GCP BigQuery API
    """

    bq_client = bigquery.Client(project=project_id)

    return bq_client


def bq_query_to_df(bq_client, query):
    """ Run query on BigQuery and return Pandas DataFrame

    Args:
        bq_client (client obj): client for GCP BigQuery API
        query (str): query to be run on BigQuery

    Returns:
        DataFrame: results from query
    """

    df = bq_client.query(query).to_dataframe()

    return df


def bq_query_to_query_job(bq_client, query):
    """ Run query on BigQuery and return QueryJob

    Args:
        bq_client (client obj): client for GCP BigQuery API
        query (str): query to be run on BigQuery

    Returns:
        QueryJob obj: results from query
    """

    query_job = bq_client.query(query)

    return query_job


def bq_df_to_table(bq_client, dataframe, table_id):
    """Load Pandas DataFrame into defined BigQuery table

    Args:
        bq_client (client obj): client for GCP BigQuery API
        dataframe (DataFrame): data to be loaded onto BigQuery
        table_id (str): ID of table for data to be loaded into
    """
    job = bq_client.load_table_from_dataframe(dataframe, table_id)
    job.result() # Wait until job complete
    return

########################################################################
# Google Cloud Storage functions

def construct_storage_client(project_id):
    """ Constructs client for GCP Cloud Storage API

    Args:
        project_id (string): name of project

    Returns:
        client obj: client for GCP Cloud Storage API
    """
    storage_client = storage.Client(project=project_id)
    return storage_client


def cs_list_folder(storage_client, src_bucket, src_folder):
    """ Lists files in bucket (and optionally folder) on Cloud Storage

    Args:
        storage_client (client obj): client for GCP Cloud Storage API
        src_bucket (str): bucket name
        src_folder (str): folder name (use empty string for no folder)

    Returns:
        List: files in location
    """

    # If given folder name is not an empty string and does not have
    # trailing /, add trailing /. Otherwise, if it is just a /, make the
    # variable an empty string. This ensure the input file name is
    # always the correct format.
    if len(src_folder) > 0 and src_folder[-1] != '/':
        src_folder = src_folder + '/'
    elif src_folder == '/':
        src_folder = ''

    # Create list of file names within location
    blobs = storage_client.list_blobs(src_bucket,
                                        prefix=src_folder,
                                        delimiter='/')
    blob_list = [blob.name for blob in blobs if blob.name != src_folder]

    return blob_list


def cs_get_object_as_list(storage_client, src_bucket, blob_name):
    """ List contents of file on Cloud Storage

    Args:
        storage_client (client obj): client for GCP Cloud Storage API
        src_bucket (str): bucket name
        blob_name (str): file relative path in bucket

    Returns:
        list: contents of file
    """

    bucket = storage_client.bucket(src_bucket)
    blob = bucket.blob(blob_name)
    text = blob.download_as_text()
    list = text.splitlines()

    return list


def cs_move_object(
    storage_client, src_bucket, blob_name, dst_bucket, dst_folder):
    """ Moves object between buckets and/or folders on Cloud Storage

    Blob assumes destination ACL

    Args:
        storage_client (client obj): client for GCP Cloud Storage API
        src_bucket (str): source bucket name
        blob_name (str): file relative path in bucket
        dst_bucket (str): destination bucket name
        dst_folder (str): destination folder name
    """

    # Construct blob object
    src_bucket_obj = storage_client.bucket(src_bucket)
    blob = src_bucket_obj.blob(blob_name)

    # If given destination folder name is not an empty string and does
    # not have trailing /, add trailing /. Otherwise, if it is just a /,
    # make the variable an empty string. This ensure the input file name
    # is always the correct format.
    if len(dst_folder) > 0 and dst_folder[-1] != '/':
        dst_folder = dst_folder + '/'
    elif dst_folder == '/':
        dst_folder = ''

    # Get relative path of file
    file_name = os.path.basename(blob.name)

    # If moving location within same bucket, rename file relative path
    # with required prefix. Otherwise, when changing bucket, copy file
    # and delete original.
    if src_bucket == dst_bucket:
        src_bucket_obj.rename_blob(blob, dst_folder + file_name)
    else:
        dst_bucket_obj = storage_client.bucket(dst_bucket)
        src_bucket_obj.copy_blob(blob, dst_bucket_obj, dst_folder + file_name)
        blob.delete()
    return