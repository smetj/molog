#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  molog
#
#  Copyright 2012 Jelle Smet development@smetj.net
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#
#

from wishbone import Wishbone
from wishbone.server import ParallelServer
from logging import DEBUG, INFO
import argparse

class MologSetup():

    def __init__(self, broker, validateLogStashData, store_es, matches, nagiosCheckResult):
        self.broker=broker
        self.validateLogStashData=validateLogStashData
        self.store_es=store_es
        self.matches=matches
        self.nagiosCheckResult=nagiosCheckResult
        self.setup()

    def setup(self):

        wb = Wishbone()

        wb.registerModule ( ('wishbone.io_modules.broker', 'Broker', 'broker'), **self.broker )
        wb.registerModule ( ('wishbone.modules.jsonvalidator', 'JSONValidator', 'validateLogStashData'), **self.validateLogStashData )
        wb.registerModule ( ('molog', 'StoreES', 'store_es'), **self.store_es )
        wb.registerModule ( ('molog', 'Matches', 'matches'), **self.matches )
        wb.registerModule ( ('molog', 'NagiosCheckResult', 'nagiosCheckResult'), **self.nagiosCheckResult )

        #Connecting the dots
        wb.connect (wb.broker.inbox, wb.validateLogStashData.inbox)
        wb.connect (wb.validateLogStashData.outbox, wb.matches.inbox)
        wb.connect (wb.matches.outbox, wb.store_es.inbox)
        wb.connect (wb.store_es.outbox, wb.nagiosCheckResult.inbox)
        wb.connect (wb.nagiosCheckResult.outbox, wb.broker.outbox)
        wb.start()

def main ():

    parser = argparse.ArgumentParser()
    parser.add_argument('command', nargs=1, help='Which command to issue.  start, stop, status or debug.')
    parser.add_argument('--config', dest='config', help='The location of the configuration file.')
    parser.add_argument('--instances', dest='instances', default=1, help='The number of parallel instances to start.')
    cli=vars(parser.parse_args())
    ParallelServer(instances=cli['instances'], setup=MologSetup, command=cli['command'][0], config=cli['config'], name='molog', log_level=INFO)

if __name__ == '__main__':
    main()
