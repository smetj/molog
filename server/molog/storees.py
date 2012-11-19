#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#       molog.py
#       
#       Copyright 2012 Jelle Smet development@smetj.net
#       
#       This program is free software; you can redistribute it and/or modify
#       it under the terms of the GNU General Public License as published by
#       the Free Software Foundation; either version 3 of the License, or
#       (at your option) any later version.
#       
#       This program is distributed in the hope that it will be useful,
#       but WITHOUT ANY WARRANTY; without even the implied warranty of
#       MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#       GNU General Public License for more details.
#       
#       You should have received a copy of the GNU General Public License
#       along with this program; if not, write to the Free Software
#       Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#       MA 02110-1301, USA.
#       
#       

import json
from wishbone.toolkit import PrimitiveActor
from wishbone.tools.estools import ESTools
from datetime import date
from gevent import monkey; monkey.patch_all()


class StoreES(PrimitiveActor, ESTools):
    '''SToreES is a Wisbone module which stores incoming data into ElasticSearch using the LogStash index naming schema.
    
    Parameters:        

        * host: The ES host to connect to.
        * user: The username to authenticate.
        * password: The password to authenticate.
    
    '''
    
    def __init__(self, name, host="localhost:9200"):
        PrimitiveActor.__init__(self, name)
        self.host = host
        self.setupConnection()
        
    def consume(self, doc):
        self.es_index(json.dumps(doc["data"]),"logstash-%d.%02d.%02d"%(date.today().year, date.today().month, date.today().day),doc['data']['@type'])
        self.sendData(doc)

class Filter(PrimitiveActor):
    '''Filter is a Wishbone module which matches chains of regexes stored in MongoDB against the incoming data.
    
    Data which is matched is passing through, all other data is purged.'''
    
    def __init__(self, name, *args, **kwargs):
        primitiveActor.__init__(self, name)
        
    def consume(self, doc):
        pass
    
