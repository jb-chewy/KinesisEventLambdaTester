import boto3
import os
import sys
import contextlib
import json
from datetime import datetime
import time
from argparse import ArgumentParser
import tzlocal

mod_name_placeholder = 'placeholder.name'


def main(stream_name, lambda_file, lambda_handler):
    kinesis_client = boto3.client('kinesis', endpoint_url='http://localhost:4567')
    response = kinesis_client.describe_stream(StreamName=stream_name)
    my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = kinesis_client.get_shard_iterator(StreamName=stream_name,
                                                          ShardId=my_shard_id,
                                                          ShardIteratorType='LATEST')
    my_shard_iterator = shard_iterator['ShardIterator']
    record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator,
                                                  Limit=2)
    while 'NextShardIterator' in record_response:
        record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'],
                                                      Limit=2)
        json_records_str = json.dumps(record_response, default=str)
        json_records = json.loads(json_records_str)
        print(json_records)
        try:
            lambda_mod = import_lambda_module(mod_name_placeholder, lambda_file)
            handler_def = getattr(lambda_mod, lambda_handler)
            handler_def(record_response, '')
        except Exception as e:
            print("Error!")
            print(e)
        # wait for 5 seconds
        time.sleep(5)

def import_lambda_module(full_name, path):
    """Import a python module from a path. 3.4+ only.
    Does not call sys.modules[full_name] = path
    """
    from importlib import util

    spec = util.spec_from_file_location(full_name, path)
    mod = util.module_from_spec(spec)

    spec.loader.exec_module(mod)
    return mod

if __name__== "__main__":
    parser = ArgumentParser()
    parser.add_argument("-s", "--stream", dest="stream_name", help="name of the local stream")
    parser.add_argument("-l", "--lambda", dest="lambda_name", help="name of the lambda to invoke")
    parser.add_argument("--handler", dest="l_handler", help="handler method for lambda")
    args = parser.parse_args()
    main(args.stream_name, args.lambda_name, args.l_handler)