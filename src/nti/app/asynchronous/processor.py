#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id: processor.py 122008 2017-09-21 00:17:03Z carlos.sanchez $
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import os
import sys
import signal
import logging
import argparse

from zope import component

from zope.exceptions.log import Formatter as zope_formatter

from nti.asynchronous.interfaces import IQueue
from nti.asynchronous.interfaces import IRedisQueue
from nti.asynchronous.interfaces import IAsyncReactor

from nti.asynchronous.reactor import DEFAULT_TRX_SLEEP
from nti.asynchronous.reactor import DEFAULT_TRX_RETRIES
from nti.asynchronous.reactor import DEFAULT_MAX_UNIFORM
from nti.asynchronous.reactor import DEFAULT_MAX_SLEEP_TIME

from nti.asynchronous.reactor import AsyncReactor
from nti.asynchronous.reactor import ThreadedReactor
from nti.asynchronous.reactor import AsyncFailedReactor

from nti.asynchronous.redis_queue import RedisQueue
from nti.asynchronous.redis_queue import PriorityQueue

from nti.asynchronous.scheduled.redis_queue import ScheduledQueue

from nti.dataserver.interfaces import IRedisClient
from nti.dataserver.interfaces import IDataserverTransactionRunner

from nti.dataserver.utils import run_with_dataserver

from nti.dataserver.utils.base_script import create_context

DEFAULT_LOG_FORMAT = '%(asctime)s %(levelname)-5.5s [%(name)s][%(thread)d][%(threadName)s] %(message)s'

logger = __import__('logging').getLogger(__name__)


# signal handlers


def handler(*unused_args):
    raise SystemExit()


def sigint_handler(*unused_args):
    logger.info("Shutting down %s", os.getpid())
    sys.exit(0)
signal.signal(signal.SIGTERM, handler)
signal.signal(signal.SIGINT, sigint_handler)


class Processor(object):

    conf_package = 'nti.appserver'

    processor_name = "Async processor"

    def add_arg_parser_arguments(self, arg_parser):
        arg_parser.add_argument('-v', '--verbose', help="Be verbose",
                                action='store_true', dest='verbose')

        # config context
        arg_parser.add_argument('-l', '--library', help="Load library packages",
                                action='store_true', dest='library')
        arg_parser.add_argument('--slugs',
                                help="Load context slugs",
                                dest='slugs',
                                action='store_true')

        # reactor queues
        arg_parser.add_argument('-n', '--name', help="Queue name",
                                default=u'', dest='name')
        arg_parser.add_argument('-q', '--queue_names', help="Queue names",
                                default='', dest='queue_names')

        # reactor settings
        arg_parser.add_argument('--redis', help="Use redis queues",
                                action='store_true', dest='redis')
        arg_parser.add_argument('--failed_jobs', help="Process failed jobs",
                                action='store_true', dest='failed_jobs')

        # reactor type redis type
        arg_parser.add_argument('-t', '--threaded', help="Threaded reactor",
                                action='store_true', dest='threaded')
        arg_parser.add_argument('--priority', help="Priority redis queue",
                                action='store_true', dest='priority')

        # reactor loop settings
        arg_parser.add_argument('--no_exit', help="Whether to exit on errors",
                                default=True, dest='exit_error', action='store_false')
        arg_parser.add_argument('-r', '--max_range_uniform',
                                help="Max sleep range tic when no jobs",
                                default=DEFAULT_MAX_UNIFORM,
                                dest='max_range_uniform',
                                type=int)
        arg_parser.add_argument('-s', '--max_sleep_time',
                                help="Max sleep time when no jobs",
                                default=DEFAULT_MAX_SLEEP_TIME,
                                dest='max_sleep_time',
                                type=int)

        # transaction runner
        arg_parser.add_argument('--trx_sleep',
                                help="Transaction sleep",
                                default=DEFAULT_TRX_SLEEP,
                                dest='trx_sleep',
                                type=int)
        arg_parser.add_argument('--trx_retries',
                                help="Max number of transaction retries",
                                default=DEFAULT_TRX_RETRIES,
                                dest='trx_retries',
                                type=int)
        arg_parser.add_argument('--site', dest='site', help="Application SITE")
        return arg_parser

    def create_arg_parser(self):
        arg_parser = argparse.ArgumentParser(description=self.processor_name)
        return self.add_arg_parser_arguments(arg_parser)

    def set_log_formatter(self, *unused_args):
        ei = DEFAULT_LOG_FORMAT
        logging.root.handlers[0].setFormatter(zope_formatter(ei))

    def setup_redis_queues(self, queue_names, clazz=RedisQueue):
        all_queues = list(queue_names)
        gsm = component.globalSiteManager
        redis = component.getUtility(IRedisClient)
        for name in all_queues:
            queue = clazz(redis, name)
            gsm.registerUtility(queue, IRedisQueue, name)

    def load_library(self):
        try:
            from nti.contentlibrary.interfaces import IContentPackageLibrary
            library = component.queryUtility(IContentPackageLibrary)
            if library is not None:
                library.syncContentPackages()
        except ImportError:
            logger.debug("Library not available")

    def create_reactor(self, failed_jobs=False, threaded=False, exit_on_error=False, **kwargs):
        if failed_jobs:
            target = AsyncFailedReactor(**kwargs)
            component.globalSiteManager.registerUtility(target, IAsyncReactor)
        elif not threaded:
            target = AsyncReactor(exitOnError=exit_on_error, **kwargs)
            component.globalSiteManager.registerUtility(target, IAsyncReactor)
        else:
            target = ThreadedReactor(**kwargs)
            component.globalSiteManager.registerUtility(target, IAsyncReactor)
        return target

    def process_args(self, args):
        self.set_log_formatter(args)

        name = getattr(args, 'name', None) or ''
        queue_names = getattr(args, 'queue_names', None)

        if not name and not queue_names:
            raise ValueError('No queue name(s) passed in')

        if name and not queue_names:
            queue_names = [name]

        if getattr(args, 'redis', False):
            queue_interface = IRedisQueue
            logger.info("Using redis queues")
            if getattr(args, 'scheduled', False):
                self.setup_redis_queues(queue_names, clazz=ScheduledQueue)
            elif getattr(args, 'priority', False):
                self.setup_redis_queues(queue_names, clazz=PriorityQueue)
            else:
                self.setup_redis_queues(queue_names, clazz=RedisQueue)
        else:
            queue_interface = IQueue

        failed_jobs = getattr(args, 'failed_jobs', False)

        if getattr(args, 'library', False):
            runner = component.getUtility(IDataserverTransactionRunner)
            runner(self.load_library)
            logger.info("Library loaded")

        site = getattr(args, 'site', None)
        if site:
            logger.info("Using site %s", site)
        site_names = (site,) if site else ()

        threaded = getattr(args, 'threaded', False)
        exit_on_error = getattr(args, 'exit_error', True)

        max_sleep_time = getattr(args, 'max_sleep_time')
        max_range_uniform = getattr(args, 'max_range_uniform')

        trx_sleep = getattr(args, 'trx_sleep')
        trx_retries = getattr(args, 'trx_retries')

        kwargs = {
            'site_names': site_names,
            'trx_sleep': trx_sleep,
            'trx_retries': trx_retries,
            'queue_names': queue_names,
            'queue_interface': queue_interface,
            'max_sleep_time': max_sleep_time,
            'max_range_uniform': max_range_uniform,
        }

        target = self.create_reactor(failed_jobs, threaded, exit_on_error, **kwargs)
        result = target()
        sys.exit(result)

    def extend_context(self, context):
        pass

    def create_context(self, env_dir, args):
        slugs = getattr(args, 'slugs', False)
        context = create_context(env_dir,
                                 slugs=slugs,
                                 plugins=slugs,
                                 with_library=True)
        self.extend_context(context)
        return context

    def conf_packages(self):
        return (self.conf_package, 'nti.app.asynchronous')

    def __call__(self, **unused_kwargs):
        arg_parser = self.create_arg_parser()
        args = arg_parser.parse_args()

        env_dir = os.getenv('DATASERVER_DIR')
        env_dir = os.path.expanduser(env_dir) if env_dir else env_dir
        if     not env_dir \
            or not os.path.exists(env_dir) \
            and not os.path.isdir(env_dir):
            raise IOError("Invalid dataserver environment root directory")

        context = self.create_context(env_dir, args)
        conf_packages = self.conf_packages()

        run_with_dataserver(environment_dir=env_dir,
                            xmlconfig_packages=conf_packages,
                            verbose=args.verbose,
                            context=context,
                            minimal_ds=True,
                            use_transaction_runner=False,
                            function=lambda: self.process_args(args))
    run = __call__


def main():
    return Processor()()


if __name__ == '__main__':
    main()
