from __future__ import absolute_import
try:
    from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse
import tempfile
import urllib2
import zipfile
import os

import requests

CHUNK_SIZE = 1024


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
