#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import component

from z3c.autoinclude.zcml import includePluginsDirective

from nti.app.asynchronous.processor import Processor

from nti.app.asynchronous.scheduled.reactor import ScheduledReactor

from nti.asynchronous.scheduled import SCHEDULED_QUEUE_NAMES
from nti.asynchronous.scheduled import NOTIFICATION_QUEUE_NAME

from nti.asynchronous.interfaces import IAsyncReactor


class Constructor(Processor):

    def create_reactor(self, failed_jobs=False, threaded=False, exit_on_error=False, **kwargs):
        target = ScheduledReactor(NOTIFICATION_QUEUE_NAME, **kwargs)
        component.globalSiteManager.registerUtility(target, IAsyncReactor)
        return target

    def process_args(self, args):
        setattr(args, 'redis', True)
        setattr(args, 'scheduled', True)
        setattr(args, 'queue_names', SCHEDULED_QUEUE_NAMES)
        super(Constructor, self).process_args(args)


def main():
    return Constructor()()


if __name__ == '__main__':
    main()
