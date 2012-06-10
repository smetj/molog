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
from gevent import monkey; monkey.patch_socket()

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
        self.connection = self.__setupDB()
        self.chains = self.connection.molog.chains
        print "x"
    
    def consume(self,doc):
        '''For each message received, run through all defined chains and look for a match.'''
        for chain in self.chains.find():
            if self.__checkMatch(chain, doc):
                self.logging.info('Match')
                print doc['header']['es_reference']
                self.sendData(doc)
            else:
                self.logging.info('No Match')                
    
    def __checkMatch(self, chain, doc):
        '''For each regex in the chain, check wether it's intended for a root key or @fields.'''
        for regex in chain['regexes']:
            if regex['field'].startswith('@') and doc['data'].has_key(regex['field']):
                if self.__match( regex, doc['data'][regex['field']] ) == False:
                    return False
            elif doc['data']['@fields'].has_key(regex['field']):
                for value in doc['data']['@fields'][regex['field']]:
                    if self.__match( regex, value ) == False:
                        return False
            else:
                return False                    
        return True                    
            
    def __match(self, regex, data):
        '''Do some actual regex matching.'''
        if regex['type'] == 'positive':
            if match(regex['regex'], data):
                self.logging.info ('%s matches %s' % (data, regex['regex']))
                return True
        elif rule['type'] == 'negative':
            if not match(regex['regex'], data):
                self.logging.info ('%s does not match %s' % (data, regex['regex']))
                return True
        return False

    def shutdown(self):
        self.connection.close()
        self.logging.info('Shutdown')

    def __setupDB(self):
        '''Setup a MongoDB connection.'''
        return Connection( self.host, self.port, use_greenlets=True )
            
        
        
