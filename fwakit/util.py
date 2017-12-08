from __future__ import absolute_import
try:
    from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse
import tempfile
import urllib2
import zipfile
import os
import pkg_resources
import logging as lg
import datetime as dt
import sys
import unicodedata

import yaml
import requests

import pgdb
import fwakit as fwa

CHUNK_SIZE = 1024


def read_config(config_file):
    """ Return config yaml as dict
    """
    with open(config_file) as cf:
        config = yaml.load(cf)
    return config


def load_queries():
    """ Load sql queries to dict
    """
    queries = {}
    for f in pkg_resources.resource_listdir(__name__, "sql"):
        key = os.path.splitext(f)[0]
        queries[key] = pkg_resources.resource_string(__name__,
                           os.path.join("sql", f))
    return queries


def connect(db_url=None):
    if not db_url:
        db_url=fwa.config['db_url']
    return pgdb.connect(db_url)


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
    if parsed_url.scheme == "http" or parsed_url.scheme == "https":
        res = requests.get(url, stream=True, verify=False)

        if not res.ok:
            raise IOError

        for chunk in res.iter_content(CHUNK_SIZE):
            fp.write(chunk)
    # ftp
    elif parsed_url.scheme == "ftp":
        download = urllib2.urlopen(url)
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

    from osmnx: https://github.com/gboeing/osmnx
    """

    if level is None:
        level = fwa.config.log_level
    if name is None:
        name = fwa.config.log_name
    if filename is None:
        filename = fwa.config.log_filename

    # if logging to file is turned on
    if fwa.config.log_file:
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
    if fwa.config.log_console:
        # capture current stdout, then switch it to the console, print the
        # message, then switch back to what had been the stdout. this prevents
        # logging to notebook - instead, it goes to console
        standard_out = sys.stdout
        sys.stdout = sys.__stdout__

        # convert message to ascii for console display so it doesn't break
        # windows terminals
        message = unicodedata.normalize('NFKD', make_str(message)).encode('ascii', errors='replace').decode()
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
        level = fwa.config.log_level
    if name is None:
        name = fwa.config.log_name
    if filename is None:
        filename = fwa.config.log_filename

    logger = lg.getLogger(name)

    # if a logger with this name is not already set up
    if not getattr(logger, 'handler_set', None):

        # get today's date and construct a log filename
        todays_date = dt.datetime.today().strftime('%Y_%m_%d')
        log_filename = '{}/{}_{}.log'.format(fwa.config.logs_folder, filename, todays_date)

        # if the logs folder does not already exist, create it
        if not os.path.exists(fwa.config.logs_folder):
            os.makedirs(fwa.config.logs_folder)

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
