#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#       matches.py
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

from re import match
from wishbone.toolkit import PrimitiveActor
from pymongo import Connection
from gevent.monkey import patch_all; patch_all()

class Matches(PrimitiveActor):
    '''Reads all defined regex chains from MongoDB and applies them to the received data to see if we have a match.

    Unmatched records are dropped.
    
    Parameters:
    
        * name: 
    '''
    
    def __init__(self, name, *args, **kwargs):
        PrimitiveActor.__init__(self, name)
        self.host = kwargs.get('host','localhost')
        self.port = kwargs.get('port',27017)
        self.chains = self.__setupDB()
    
    def consume(self,doc):
        #print doc["data"]["@fields"]
        for chain in self.chains.find():
            if self.__checkMatch(chain, doc):
                self.logging.info('Match')
                self.sendData(doc)
            else:
                self.logging.info('No Match')                
    
    def __checkMatch(self, chain, doc):
            for regex in chain['regexes']:
                if doc['data'].has_key(regex['field']):
                    if regex['type'] == 'positive':
                        if match(regex['regex'], doc['data'][regex['field']]):
                            self.logging.info ('%s matches %s' % (doc['data'][regex['field']], regex['regex']))
                        else:
                            pass
                    elif rule['type'] == 'negative':
                        if not match(regex['regex'], doc['data'][regex['field']]):
                            self.logging.info ('%s does not match %s' % (doc['data'][regex['field']], regex['regex']))
                        else:
                            return False
                else:
                    return False
            return True                    

    def shutdown(self):
        self.logging.info('Shutdown')

    def __setupDB(self):
        return Connection( self.host, self.port).molog.chains
            
        
        
