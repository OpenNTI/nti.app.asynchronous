#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope.cachedescriptors.property import readproperty

from nti.asynchronous.reactor import AsyncReactor

from nti.asynchronous.scheduled.utils import get_executor_queue

logger = __import__('logging').getLogger(__name__)


class ScheduledReactor(AsyncReactor):

    def __init__(self, target_queue_name, **kwargs):
        self.target_queue_name = target_queue_name
        AsyncReactor.__init__(self, **kwargs)

    @readproperty
    def target_queue(self):
        return get_executor_queue(name=self.target_queue_name)

    def perform_job(self, job, queue=None):
        queue = self.current_queue if queue is None else queue
        logger.debug("[%s] Moving job (%s)", queue, job)

        target_queue = self.target_queue
        if target_queue is None:
            return False

        self.target_queue.put(job)

        logger.info("[%s] Job %s has been moved to queue(%s).", queue, job.id, self.target_queue)
        return True


class ExecutingReactor(AsyncReactor):

    def perform_job(self, job, queue=None):
        queue = self.current_queue if queue is None else queue
        logger.debug("[%s] Executing job (%s)", queue, job)

        job.run()

        if job.has_failed():
            logger.error("[%s] Job %s failed", queue, job.id)
            queue.put_failed(job)
        logger.info("[%s] Job %s has been executed (%s).",
                    queue, job.id, job.status)
        return True
