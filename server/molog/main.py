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

def setup():
        wb = Wishbone()
        wb.registerModule ( ('wishbone.io_modules.broker', 'Broker', 'broker'), host='sandbox', vhost='/', username='guest', password='guest', consume_queue='molog_input', no_ack=False )
        wb.registerModule ( ('wishbone.modules.jsonvalidator', 'JSONValidator', 'validateLogStashData'), schema='/etc/molog/broker.schema', convert=True )
        wb.registerModule ( ('molog', 'StoreES', 'store_es'), host='sandbox:9200' )
        wb.registerModule ( ('molog', 'Matches', 'matches'), host='sandbox', port=27017, warning=['3','4'], critical=['0','1','2'] )
        wb.registerModule ( ('molog', 'NagiosCheckResult', 'nagiosCheckResult'), warning=['3','4'], critical=['0','1','2'], exchange='', routing_key='nagios_check_results' )

        #Connecting the dots
        wb.connect (wb.broker.inbox, wb.validateLogStashData.inbox)
        wb.connect (wb.validateLogStashData.outbox, wb.matches.inbox)
        wb.connect (wb.matches.outbox, wb.store_es.inbox)
        wb.connect (wb.store_es.outbox, wb.nagiosCheckResult.inbox)
        wb.connect (wb.nagiosCheckResult.outbox, wb.broker.outbox)        
        
        wb.start()
        
def main():        
    server = ParallelServer(instances=5, setup=setup, daemonize=False, name='moncli', log_level=INFO)
    server.start()

if __name__ == '__main__':
    main()
