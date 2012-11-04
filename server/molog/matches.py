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
from json import dumps
from wishbone.toolkit import PrimitiveActor
from wishbone.tools.mongotools import MongoTools

class Matches(PrimitiveActor,MongoTools):
    '''Reads all defined regex chains from MongoDB and tags documents according to them.

    Parameters:
    
        * name: the name of this module.
        * host: The hostname on which MongoDB is listening
        * port: The port on which MongoDB is listening.
        * database: The database containing the collection.
        * collection: The collection containing the chains.
        * prefix: The prefix of the fields added to processed documents.
    '''
    
    def __init__(self, name, *args, **kwargs):
        PrimitiveActor.__init__(self, name)
        self.host = kwargs.get('host','localhost')
        self.port = kwargs.get('port',27017)
        self.database = kwargs.get('database','molog')
        self.collection = kwargs.get('chains','chains')
        self.prefix = kwargs.get('prefix','molog')
        self.setupConnection()
        
    def consume(self,doc):
        
        '''For each message received, run through all defined chains and look for a match.'''
        
        for chain in self.conn[self.database][self.collection].find():
            if self.__checkMatch(chain, doc):
                self.extendDocument(doc, chain['tags'], chain['name'])
                doc['header']['matches']=chain['name']
            else:
                self.logging.debug ('%s - No match for %s' % (chain['name'],dumps(doc['data'])))
        self.sendData(doc)
    
    def __checkMatch(self, chain, doc):
        
        '''For each regex in the chain, check wether it's intended for a root key or @fields.'''
        
        for regex in chain['regexes']:
            if regex['field'].startswith('@') and doc['data'].has_key(regex['field']):
                if self.__match(chain['name'],regex, doc['data'][regex['field']] ) == False:
                    return False
            elif doc['data']['@fields'].has_key(regex['field']):
                if isinstance(doc['data']['@fields'][regex['field']],list):
                    for value in doc['data']['@fields'][regex['field']]:
                        if self.__match(chain['name'],regex, value ) == False:
                            return False
                else:
                    if self.__match(chain['name'],regex, doc['data']['@fields'][regex['field']] ) == False:
                            return False
            else:
                self.logging.debug ('Field %s could not be found for message %s'%(regex['field'],dumps(doc['data'])))
                return False
        return True                    
            
    def __match(self, name, regex, data):
        
        '''Do some actual regex matching.'''
        
        if regex['type'] == 'include':
            if match(regex['regex'], str(data)):
                self.logging.debug ('%s - Include match using %s: %s' % (name, regex['regex'], data))
                return True
        elif regex['type'] == 'exclude':
            if not match(regex['regex'], str(data)):
                self.logging.debug ('%s - Exclude match using %s: %s' % (name, regex['regex'], data))
                return True
        else:
            self.logging.debug ('%s is not a valid regex type.' % (regex['type']))
        return False

    def extendDocument(self,doc, tags, name):
        
        '''Extends a document with MoLog specific data.
        I initially planned to add a dictionary, but that would limit the query possibilities through Kibana.
        Kibana also doesn't seem to cope well with assigning True/False values, so I made this a string value instead.
        
        The best of all, there appears to be a bug in ElasticSearch:
            https://github.com/elasticsearch/elasticsearch/issues/2293
        
        That's why *HAVE* to use molog_ack and not @molog_ack
        '''
        
        doc['data']['@%s_chain'%self.prefix] = name
        doc['data']['@%s_tags'%self.prefix] = tags
        doc['data']['@%s_ack'%self.prefix] = 'false'
    
    def shutdown(self):
        try:
            self.conn.close()
        except:
            pass
        self.logging.info('Shutdown')
