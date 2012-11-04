#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#       nagioscheckresult.py
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

from wishbone.toolkit import PrimitiveActor
from time import time


class NagiosCheckResult(PrimitiveActor):
    '''A class which created a Nagios compliant external check result out of the document.

    Parameters:
    
        * exchange: The name of the broker exchange to which data is submitted.
        * routing_key: the routing key 
        * warning: A list containing the syslog priorities for a Nagios warning.
        * critical: A list containing the syslog priorities for a Nagios critical.
    '''
    
    def __init__(self, name, *args, **kwargs):
        
        PrimitiveActor.__init__(self, name)
        self.exchange = kwargs.get('exchange','')
        self.routing_key = kwargs.get('routing_key','')
        self.warning = kwargs.get('warning',['3','4'])
        self.critical = kwargs.get('critical',['0','1','2'])
    
    def consume(self,doc):
        doc['header']['broker_exchange']=self.exchange
        doc['header']['broker_key']=self.routing_key
        #(numeric_status, word_status) = self.getStatus(doc['header']['warning'],doc['header']['critical'])
        #nagios_format = '[%s] PROCESS_SERVICE_CHECK_RESULT;%s;%s;%s;%s - %s warnings, %s criticals'%(time(),doc['data']['@fields']['logsource'][0],doc['header']['name'],numeric_status,word_status,doc['header']['warning'],doc['header']['critical'])
        self.sendData(doc)
        
    def getStatus(self,warning,critical):
        if warning == '0' and critical == '0':
            return ('0','OK')
        if critical != '0':
            return ('2','Critical')
        if warning != '0':
            return ('1','Warning')
        
    def countMongo(self,host):
        '''Counting the amount of objects we already have referenced.'''

        return ( self.conn.molog.references.find({'hostname':host,'priority': { "$in":self.warning}}).count(), 
        self.conn.molog.references.find({'hostname':host,'priority': { "$in":self.critical}}).count() )            
        
        
        
