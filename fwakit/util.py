# logging, download, file utils
# logging and settings code is from https://github.com/gboeing/osmnx

from __future__ import absolute_import
try:
    from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse
try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen


import datetime as dt
import logging as lg
import os
import pkg_resources
import sys
import tempfile
import unicodedata
import zipfile

import requests

import pgdata

from . import settings

CHUNK_SIZE = 1024


class QueryDict(object):
    """Provide a dict like interface to files in the /sql folder
    """
    def __init__(self):
        self.queries = None

    def __getitem__(self, query_name):
        if pkg_resources.resource_exists(
            __name__,
            os.path.join("sql", query_name+'.sql')):
            return pkg_resources.resource_string(
                __name__,
                os.path.join("sql", query_name+'.sql')).decode('utf-8')

        else:
            raise ValueError("Invalid query name: %r" % query_name)


def config(source_url=settings.source_url,
           dl_path=settings.dl_path,
           source_tables=settings.source_tables,
           log_file=settings.log_file,
           log_console=settings.log_console,
           log_level=settings.log_level,
           log_name=settings.log_name,
           log_filename=settings.log_filename):
    """
    Configure fwakit by setting the default global vars to desired values.
    Parameters
    ---------
    source_url : string
        url from where to download FWA .gdb files
    dl_path : string
        where to write the downloaded FWA .gdb files
    db_url : string
        SQLAlchemy db connection string for postgres database
    sources_dict : dict
        A dictionary defining source data files, see settings.py
    log_file : bool
        if true, save log output to a log file in logs_folder
    log_console : bool
        if true, print log output to the console
    log_level : int
        one of the logger.level constants
    log_name : string
        name of the logger
    Returns
    -------
    None
    """

    # set each global variable to the passed-in parameter value
    settings.source_url = source_url
    settings.dl_path = dl_path
    settings.source_tables = source_tables
    settings.log_console = log_console
    settings.log_file = log_file
    settings.log_level = log_level
    settings.log_name = log_name
    settings.log_filename = log_filename

    # if logging is turned on, log that we are configured
    if settings.log_file or settings.log_console:
        log('Configured fwakit')


def log(message, level=None, name=None, filename=None):
    """
    Write a message to the log file and/or print to the the console.
    Parameters
    ----------
    message : string
        the content of the message to log
    level : int
        one of the logger.level constants
    name : string
        name of the logger
    filename : string
        name of the log file
    Returns
    -------
    None
    """

    if level is None:
        level = settings.log_level
    if name is None:
        name = settings.log_name
    if filename is None:
        filename = settings.log_filename

    # if logging to file is turned on
    if settings.log_file:
        # get the current logger (or create a new one, if none), then log
        # message at requested level
        logger = get_logger(level=level, name=name, filename=filename)
        if level == lg.DEBUG:
            logger.debug(message)
        elif level == lg.INFO:
            logger.info(message)
        elif level == lg.WARNING:
            logger.warning(message)
        elif level == lg.ERROR:
            logger.error(message)

    # if logging to console is turned on, convert message to ascii and print to
    # the console
    if settings.log_console:
        # capture current stdout, then switch it to the console, print the
        # message, then switch back to what had been the stdout. this prevents
        # logging to notebook - instead, it goes to console
        standard_out = sys.stdout
        sys.stdout = sys.__stdout__

        # convert message to ascii for console display so it doesn't break
        # windows terminals
        message = unicodedata.normalize(
            'NFKD',
            make_str(message)).encode('ascii', errors='replace').decode()
        print(message)
        sys.stdout = standard_out


def get_logger(level=None, name=None, filename=None):
    """
    Create a logger or return the current one if already instantiated.
    Parameters
    ----------
    level : int
        one of the logger.level constants
    name : string
        name of the logger
    filename : string
        name of the log file
    Returns
    -------
    logger.logger
    """

    if level is None:
        level = settings.log_level
    if name is None:
        name = settings.log_name
    if filename is None:
        filename = settings.log_filename

    logger = lg.getLogger(name)

    # if a logger with this name is not already set up
    if not getattr(logger, 'handler_set', None):

        # get today's date and construct a log filename
        todays_date = dt.datetime.today().strftime('%Y_%m_%d')
        log_filename = '{}/{}_{}.log'.format(settings.logs_folder, filename, todays_date)

        # if the logs folder does not already exist, create it
        if not os.path.exists(settings.logs_folder):
            os.makedirs(settings.logs_folder)

        # create file handler and log formatter and set them up
        handler = lg.FileHandler(log_filename, encoding='utf-8')
        formatter = lg.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.handler_set = True

    return logger


def make_str(value):
    """
    Convert a passed-in value to unicode if Python 2, or string if Python 3.
    Parameters
    ----------
    value : any
        the value to convert to unicode/string
    Returns
    -------
    unicode or string
    """
    try:
        # for python 2.x compatibility, use unicode
        return unicode(value)
    except NameError:
        # python 3.x has no unicode type, so if error, use str type
        return str(value)


def make_sure_path_exists(path):
    """
    Make directories in path if they do not exist.
    Modified from http://stackoverflow.com/a/5032238/1377021
    """
    try:
        os.makedirs(path)
    except:
        pass
    return path


def download_and_unzip(url, unzip_dir):
    """Download and unzip a zipped folder from web or ftp
    """
    fp = tempfile.NamedTemporaryFile('wb',
                                     dir=tempfile.gettempdir(),
                                     suffix=".zip",
                                     delete=False)
    parsed_url = urlparse(url)
    # http
    if parsed_url.scheme == "http" or parsed_url.scheme == 'https':
        res = requests.get(url, stream=True)

        if not res.ok:
            raise IOError

        for chunk in res.iter_content(CHUNK_SIZE):
            fp.write(chunk)

    # ftp
    elif parsed_url.scheme == "ftp":
        download = urlopen(url)
        file_size_dl = 0
        block_sz = 8192
        while True:
            buffer = download.read(block_sz)
            if not buffer:
                break
            file_size_dl += len(buffer)
            fp.write(buffer)
    # unzipping won't work if file isn't closed
    fp.close()
    # unzip the file to target folder, delete zip archive
    unzip_dir = make_sure_path_exists(unzip_dir)
    zipped_file = zipfile.ZipFile(fp.name, 'r')
    zipped_file.extractall(unzip_dir)
    zipped_file.close()
    os.unlink(fp.name)
    return os.path.join(unzip_dir, os.path.split(parsed_url.path)[1])


def get_shortcuts():
    """ Return a dictionary of shortcuts to tables in settings.source_tables
    """
    aliases = {}
    tables = {}
    for source_table in settings.source_tables:
        # add schema qualified table name to table dict, alias dict
        tables[source_table['table']] = 'whse_basemapping.' + source_table['table']
        aliases[source_table['alias']] = 'whse_basemapping.' + source_table['table']
    return (tables, aliases)


def connect(db_url=None):
    if not db_url:
        db_url = os.environ['FWA_DB']
    return pgdata.connect(db_url)


def load_queries():
    """ Load queries from module /sql folder to dict
    """
    queries = {}
    for f in pkg_resources.resource_listdir(__name__, "sql"):
        key = os.path.splitext(f)[0]
        queries[key] = pkg_resources.resource_string(__name__,
                                                     os.path.join("sql", f)).decode('utf-8')
    return queries
