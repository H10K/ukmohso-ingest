#!/usr/bin/env python
"""
ukmohso-ingest.py.

Import Historical Stations Observations from the UK Met Office. See
https://www.metoffice.gov.uk/research/climate/maps-and-data/historic-station-data
for more details.
"""
import argparse
import boto3
import gzip
import logging
import os
import sentry_sdk
import sys
import urllib.request
import yaml

from pathlib import Path
from urllib.request import Request
from urllib.request import urlopen


class IngestHistoricStationData:
    """Ingest Historic Station Data."""

    def __init__(self, datafile):
        """
        Initialise the object.

        args:
            datafile - The name of the data file containing the station names.

        """
        logging.info(f'Opening {datafile}.')
        stream = open(datafile, 'r')
        data = yaml.safe_load(stream)
        stream.close()
        self.stations(data['stations'])
        self.s3bucket(data['s3bucket'])
        self.s3path(data['s3path'])

    def get_data(self, station):
        """
        Download the data from the station URL.

        args:
            station - The name of the station.

        """
        server = 'www.metoffice.gov.uk'
        path = 'pub/data/weather/uk/climate/stationdata'
        url = f'https://{server}/{path}/{station}data.txt'
        logging.debug(f'url={url}')
        req = Request(url)
        req.add_header('User-Agent', 'curl/7.64.1')
        data = urlopen(req).read()
        uncompressed_data_length = len(data)
        logging.debug(f'Downloaded {uncompressed_data_length} bytes')
        gzip_file_name = f'{station}data.txt.gz'
        logging.debug(f'Writing GZIP archive to {gzip_file_name}.')
        f = gzip.open(gzip_file_name, 'wb')
        f.write(data)
        f.close()
        compressed_data_length = os.path.getsize(gzip_file_name)
        percentage = compressed_data_length / uncompressed_data_length
        percentage *= 100.0
        percentage = 100.0 - percentage
        logmsg = 'Compressed down to %d bytes (%.02f%%).' % (
            compressed_data_length,
            percentage)
        logging.debug(logmsg)
        s3object = f'{self.s3path()}/{gzip_file_name}'
        logging.debug(f'Uploading to {self.s3bucket()}://{s3object}.')
        s3 = boto3.client('s3')
        f = open(gzip_file_name, 'rb')
        s3.upload_fileobj(f, self.s3bucket(), s3object)
        f.close()
        logging.debug(f'Removing file {gzip_file_name}.')
        p = Path(gzip_file_name)
        p.unlink()

    def s3bucket(self, s3bucket=None):
        """Get/set the S3 Bucket Name."""
        if s3bucket is not None:
            self._s3bucket = s3bucket
        return self._s3bucket

    def s3path(self, s3path=None):
        """Get/set the S3 Path Name."""
        if s3path is not None:
            self._s3path = s3path
        return self._s3path

    def stations(self, stations=None):
        """
        Get or set the list of station names.

        args:
            stations - A list of station names.

        """
        if stations is not None:
            self._stations = stations
        return self._stations


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
      description='Ingest Met Office Historic Station Data')
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-d",
                       "--debug",
                       action="store_true",
                       help="Show DEBUG output.")
    parser.add_argument("-f",
                        "--file",
                        help="Specify the station data file.",
                        type=str)
    group.add_argument("-q",
                       "--quiet",
                       action="store_true",
                       help="Show WARN/ERROR output only.")
    args = parser.parse_args()

    if args.quiet:
        logging.basicConfig(level='WARN')
    elif args.debug:
        logging.basicConfig(level='DEBUG')
        logging.debug('Output at DEBUG level.')
    else:
        logging.basicConfig(level='INFO')

    if 'SENTRY_DSN' in os.environ:
        logging.info('Setting Sentry DSN.')
        sentry_sdk.init(os.environ['SENTRY_DSN'])
    else:
        logging.warn('Sentry not configured.')

    if args.file is None:
        parser.error('Please specify a file.')

    ingest = IngestHistoricStationData(args.file)

    for station in ingest.stations():
        logging.info(f'Downloading data for {station}.')
        ingest.get_data(station)
