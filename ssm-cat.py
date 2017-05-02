#!/usr/bin/env python
# -*- coding: utf-8 -*-
import jmespath
import s3fs
import boto3
import os
import datetime
import pytz
import argparse
import codecs
import pprint as pp
from diskcache import Cache
from functools import partial
S3_CACHE_DIR = '.ssm-cat/s3'
DISK_CACHE_DIR = '.ssm-cat/diskcache'
from chardet.universaldetector import UniversalDetector


def _check_encoding(file_path):
    detector = UniversalDetector()
    with open(file_path, mode='rb') as f:
        for binary in f:
            detector.feed(binary)
            if detector.done:
                break
    detector.close()
    return detector.result['encoding']


def print_colored(code, text, is_bold=False):
    if is_bold:
        code = '1;%s' % code

    print('\033[%sm%s\033[0m' % (code, text))


print_green = partial(print_colored, '32')
print_blue = partial(print_colored, '34')
print_cyan = partial(print_colored, '36')


def _get_instance_info():
    with Cache(DISK_CACHE_DIR) as c:
        if 'instance_info' in c:
            return c.get('instance_info')
        ec2 = boto3.client('ec2')
        desc = ec2.describe_instances()
        info = jmespath.search(
            'Reservations[].Instances[].[InstanceId, Tags[?Key==`Name`].Value | [0] , PrivateIpAddress, PublicIpAddress]',
            desc
            )
        val = {x[0]: x[1:] for x in info}
        c.set(
            'instance_info',
            val,
            expire=300
            )
        return val


def _get_s3_contents(path):
    # search local cache
    local_fp = "{0}/{1}".format(S3_CACHE_DIR, path)
    if not os.path.exists(local_fp):
        fs = s3fs.S3FileSystem()
        with fs.open(path) as rf:
            if not os.path.exists(os.path.dirname(local_fp)):
                os.makedirs(os.path.dirname(local_fp))
            rdata = ""
            with open(local_fp, "wb") as lf:
                rdata = rf.read()
                lf.write(rdata)

    enc = _check_encoding(local_fp)
    with codecs.open(local_fp, "rb", enc) as f:
        return f.read()


def _get_calc_iso_datetime(delta):
    sp = delta[-1:]
    num = int(delta[0:-1])

    if sp == 'd':
        ctime = datetime.datetime.now(pytz.utc) - datetime.timedelta(days=num)
    elif sp == 'h':
        ctime = datetime.datetime.now(pytz.utc) - datetime.timedelta(hours=num)
    elif sp == 'm':
        ctime = datetime.datetime.now(pytz.utc) - datetime.timedelta(minutes=num)

    ctime = ctime.replace(second=0, microsecond=0)
    return ctime.isoformat().replace('+00:00', 'Z')


def _list_commands(args):
    # build filter
    filters = []
    if args.after or args.before:
        if args.after:
            filters.append({'key': 'InvokedAfter', 'value': args.after})
        if args.before:
            filters.append({'key': 'InvokedBefore', 'value': args.before})
    else:
        after_time = _get_calc_iso_datetime(args.delta)
        filters.append(
            {'key': 'InvokedAfter', 'value': after_time}
        )

    ssm = boto3.client('ssm')
    cmds = ssm.list_commands(MaxResults=50, Filters=filters)
    info = jmespath.search(
        'Commands[].[CommandId, Status, RequestedDateTime, length(InstanceIds), Parameters]',
        cmds
        )
    for i in info:
        print '{0[0]}\t{0[1]:<8}\t{0[2]:}\t{0[3]}\t{0[4]}'.format(i)


def _detail_command(args):
    cid = args.command
    ssm = boto3.client('ssm')
    detail = ssm.list_command_invocations(
        CommandId=cid,
        Details=True,
        MaxResults=50
        )

    info = jmespath.search(
        'CommandInvocations[].{_ins: InstanceId, _st: Status, _plugins: CommandPlugins[].{_bucket: OutputS3BucketName,_prefix: OutputS3KeyPrefix, _st:Status, _output:Output}}',
        detail
        )
    fs = s3fs.S3FileSystem()
    c = _get_instance_info()
    for i in info:
        iid = i['_ins']
        if i['_ins'] in c.keys():
            print_green("{0}\t{1[0]}\t{1[1]}\t{1[2]}\t{2}".format(iid, c[iid], i['_st']))
        else:
            print_green("{0}\t--terminated\t{1}".format(iid, i['_st']))

        for p in i['_plugins']:
            if not p['_bucket']:
                print_blue("### No s3 output")
                print p['_output']
                continue

            files = fs.walk(
                "{0:}/{1:}".format(p['_bucket'], p['_prefix'])
            )
            for fn in files:
                print_blue("### " + fn)
                print _get_s3_contents(fn)


def main():
    if not os.path.exists(S3_CACHE_DIR):
        os.makedirs(S3_CACHE_DIR)

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_ls = subparsers.add_parser('ls', help='see `ls -h`')
    parser_ls.add_argument(
        '-a',
        '--after',
        help='InvokedAfter (YYYY-MM-DDTHH:mm:SSZ UTC only)')
    parser_ls.add_argument(
        '-b',
        '--before',
        help='InvokedBefore (YYYY-MM-DDTHH:mm:SSZ UTC only)')
    parser_ls.add_argument('-d', '--delta', help='time-delta N(d,h,m)', default='1d')
    parser_ls.set_defaults(handler=_list_commands)

    parser_cat = subparsers.add_parser('cat', help='see `cat -h`')
    parser_cat.add_argument('command', help='command-id')
    parser_cat.set_defaults(handler=_detail_command)

    args = parser.parse_args()
    if hasattr(args, 'handler'):
        args.handler(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
