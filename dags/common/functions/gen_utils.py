import configparser
import os


def read_config(config_file):
    """ Reads config file

    Args:
        config_file (str): config file name

    Raises:
        OSError: if config file cannot be located

    Returns:
        ConfigParser obj: contains parsed config data
    """

    # Construct config parser
    config = configparser.ConfigParser()

    # Construct config path
    levels = 2 * '../'
    file_path = os.path.abspath(
        os.path.join(
            os.path.dirname( __file__ ), f'{levels}config/{config_file}'))

    # Read config file
    config_list = config.read(file_path)

    # If config parse is successful, return ConfigParser obj. Otherwise
    # raise error.
    if len(config_list) > 0:
        return config
    raise OSError(2, 'Could not locate config file', config_file)


def read_query(query_file_name):
    """ Returns query in sql directory as a string

    Args:
        query_file_name (str): query file name

    Returns:
        str: query as string
    """

    # Construct path to query file
    levels = 2 * '../'
    file_path = os.path.abspath(
        os.path.join(
            os.path.dirname( __file__ ), f'{levels}sql/{query_file_name}'))

    # Read file and store as string
    with open(file_path, 'r') as file:
        query_str = file.read()

    return query_str