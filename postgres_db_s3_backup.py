#!/usr/bin/env python

import subprocess
import os
import boto3
import datetime
import configparser
import sys
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("db_backup")

def run_command(command):
    out = subprocess.run(command,
                          shell=True,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,
                          check=True)
    out.check_returncode()
    return out


def check_state(expression, formatted_message, *args):
    if not expression:
        raise Exception(formatted_message.format(*args))


def check_file(filename):
    check_state(os.path.exists(filename), "The path '{}' does not exist", filename)
    check_state(os.path.isfile(filename), "The path '{}' is not a file", filename)


class DB_Dump(object):

    def __init__(self, user, password, output_dir="/tmp", host="localhost", port="5432", dump_filename_prefix="postgres_backup"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.output_dir = output_dir
        self.dump_filename_prefix = dump_filename_prefix

    @classmethod
    def _prepare_output_file(cls, output_file):
        parent_dir = os.path.dirname(output_file)
        if os.path.exists(parent_dir):
            if not os.path.isdir(parent_dir):
                raise Exception("A non-directory already has the name '{}'".format(parent_dir))
        else:
            os.makedirs(parent_dir)

    @classmethod
    def __generate_timestamp(cls):
        t = datetime.datetime.utcnow()
        return "{}{:02d}{:02d}_{:02d}{:02d}{:02d}_{}".format(t.year, t.month, t.day, t.hour, t.minute, t.second, 'UTC')

    def dump_db(self, dbname, output_file=None):
        if output_file is None:
            output_file = "{}{}{}.{}.sql.gz".format(
                self.output_dir, os.sep, self.dump_filename_prefix, DB_Dump.__generate_timestamp())

        self._prepare_output_file(output_file)
        dump_cmd = "PGPASSWORD={} pg_dump -h {}  -p {} -U {} {}".format(
            self.password, self.host, self.port, self.user, dbname)
        compress_cmd = "gzip > {}".format(output_file)
        process_output = run_command(dump_cmd+" | "+compress_cmd)
        check_file(output_file)
        return output_file

class S3Client:
    def __init__(self, endpoint_url, access_key, secret_key,
                 data_bucket):
        self.endpoint_url = endpoint_url
        self.__access_key = access_key
        self.__secret_key = secret_key
        self.data_bucket = data_bucket

        # state
        self.__client = self.__connect()
        # self.__buckets = map(lambda x: x['name'],self.__client.list_buckets()['Buckets'])
        self.__buckets = S3Client.__get_bucket_names(self.__client)

    def __connect(self):
        return boto3.client('s3',
                            aws_access_key_id=self.__access_key,
                            aws_secret_access_key=self.__secret_key,
                            use_ssl=False,
                            endpoint_url=self.endpoint_url)

    @classmethod
    def __get_bucket_names(cls, client):
        out = []
        for entry in client.list_buckets()['Buckets']:
            out.append(entry['Name'])
        return out

    def setup(self):
        self.setup_data_bucket()

    @classmethod
    def __generate_timestamp(cls):
        t = datetime.datetime.utcnow()
        return "{}{:02d}{:02d}_{:02d}{:02d}{:02d}_{}".format(t.year, t.month, t.day, t.hour, t.minute, t.second, 'UTC')

    def __generate_key(self, base_filename):
        return "{}/{}".format(self.data_bucket, base_filename)

    def upload(self, local_filename):
        check_file(local_filename)
        self.check_bucket_existence()
        base_filename = os.path.basename(local_filename)
        key = base_filename # self.__generate_key(base_filename)
        print()
        self.__client.upload_file(local_filename,
                                  self.data_bucket,
                                  key)

    def check_bucket_existence(self):
        check_state(self.is_bucket_exists(self.data_bucket), "The bucket '{}' does not exist", self.data_bucket)

    def setup_data_bucket(self):
        self.setup_bucket(self.data_bucket)

    def setup_bucket(self, name):
        if not self.is_bucket_exists(name):
            self.__client.create_bucket(Bucket=name)

    def is_bucket_exists(self, name):
        return name in self.__buckets


def main():
    ini_file = sys.argv[1]
    config = configparser.ConfigParser()
    config.read(ini_file)
    cfg = config.defaults()
    db_user = cfg['db_user']
    db_host = cfg['db_host']
    db_password = cfg['db_password']
    db_port = cfg['db_port']
    db_name = cfg['db_name']
    s3_endpoint_url = cfg['s3_endpoint_url']
    s3_data_bucket = cfg['s3_data_bucket']
    s3_access_key = cfg['s3_access_key']
    s3_secret_key = cfg['s3_secret_key']
    backup_file_prefix = cfg['backup_file_prefix']
    backup_output_dir = cfg['backup_output_dir']
    dumper = DB_Dump(db_user, db_password, host=db_host, port=db_port, dump_filename_prefix=backup_file_prefix, output_dir=backup_output_dir)
    log.info("Dumping '{}' postgres database...".format(db_name))
    output_filename = dumper.dump_db(db_name)
    log.info("\t- successfully dumped to file '{}'".format(output_filename))

    client = S3Client(s3_endpoint_url, s3_access_key, s3_secret_key, s3_data_bucket)
    log.info("Setting up buckets...")
    client.setup()
    log.info("Uploading file '{}' to s3 at endpoint '{}'...".format(output_filename, s3_endpoint_url))
    client.upload(output_filename)
    log.info("\t- successfully uploaded file '{}' to bucket '{}'".format(output_filename, s3_data_bucket))

    if os.path.exists(output_filename):
        os.remove(output_filename)
        log.info("Removed file '{}'".format(output_filename))


if __name__ == '__main__':
    main()


