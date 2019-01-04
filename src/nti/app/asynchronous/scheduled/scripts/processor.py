#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import os
import pyramid

from pyramid.interfaces import IRendererFactory

from pyramid.scripts.common import get_config_loader

from pyramid.threadlocal import get_current_registry

from pyramid_mailer import Mailer
from pyramid_mailer import IMailer

from pyramid_mako import MakoRendererFactory
from pyramid_mako import PkgResourceTemplateLookup

from zope import component

from z3c.autoinclude.zcml import includePluginsDirective

from nti.app.asynchronous.processor import Processor

from nti.asynchronous.scheduled import NOTIFICATION_QUEUE_NAMES

from nti.app.pyramid_zope.z3c_zpt import renderer_factory as pt_renderer_factory

from nti.asynchronous.interfaces import IAsyncReactor

from nti.app.asynchronous.scheduled.reactor import ProcessingReactor


def _mak_renderer_factory():
    factory = MakoRendererFactory()
    factory.lookup = PkgResourceTemplateLookup()
    return factory


def _get_app_settings():
    etc = os.getenv('DATASERVER_ETC_DIR') or os.path.join(env_dir, 'etc')
    etc = os.path.expanduser(etc)
    config_uri = os.path.join(etc, 'pserve.ini')
    loader = get_config_loader(config_uri)
    return loader.get_wsgi_app_settings('dataserver')


class Constructor(Processor):

    def _setup_template_renderers(self):
        assert get_current_registry() is pyramid.registry.global_registry
        pyramid_registry = pyramid.registry.global_registry

        for name, renderer_factory in ((u'.pt', pt_renderer_factory),
                                       (u'.mak', _mak_renderer_factory())):
            if pyramid_registry.queryUtility(IRendererFactory, name=name) is None:
                pyramid_registry.registerUtility(renderer_factory, IRendererFactory, name=name)
            assert pyramid_registry.queryUtility(IRendererFactory, name=name) is not None, u'Must provide renderer for name' % name

    def _setup_mailer(self, settings):
        if component.getGlobalSiteManager().queryUtility(IMailer) is None:
            mailer = Mailer.from_settings(settings)
            assert mailer, u'Must provide a mailer.'
            component.getGlobalSiteManager().registerUtility(mailer, IMailer)

        assert component.getGlobalSiteManager().queryUtility(IMailer), u'Must provide a mailer utility'

    def create_reactor(self, failed_jobs=False, threaded=False, exit_on_error=False, **kwargs):
        target = ProcessingReactor(**kwargs)
        component.globalSiteManager.registerUtility(target, IAsyncReactor)
        return target

    def process_args(self, args):
        setattr(args, 'redis', True)
        setattr(args, 'priority', True)
        setattr(args, 'queue_names', NOTIFICATION_QUEUE_NAMES)

        self._setup_template_renderers()

        settings = _get_app_settings()

        self._setup_mailer(settings)

        super(Constructor, self).process_args(args)


def main():
    return Constructor()()


if __name__ == '__main__':
    main()
