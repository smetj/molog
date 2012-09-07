# -*- coding: utf-8 -*-
#
#  molog_api.py
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

# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.
#
# Run this application with:
#
#   $ gunicorn multiapp:app
#
# And then visit:
#
#   http://127.0.0.1:8000/app1url
#   http://127.0.0.1:8000/app2url
#   http://127.0.0.1:8000/this_is_a_404
#

try:
    from routes import Mapper
except:
    print "This example requires Routes to be installed"
import json
import sys
import inspect
import urlparse
from pymongo import Connection
from bson.objectid import ObjectId

class MologTools():
    
    def buildQuery(self,dict, list):
        query={}
        if dict.has_key('id'):
            return {'_id':dict['id']}
        for param in list:
            if dict.has_key(param) and dict[param] != None:
                query[param]=dict[param]
        return query
    
    def generateJSON(self, metrics={}, result=[]):
        data=[]
        for item in result:
            if item.has_key('_id'):
                item['id']=str(item['_id'])
                del(item['_id'])
                data.append(item)
        return json.dumps({'metrics':metrics, 'data':data})
    
    def getRecords(self, query={}, limit=0):
        '''Queries MongoDB and returns records based upon the query passed.
        '''
        result=[]
        metrics={'total':self.db.references.find(query).count()}
        for reference in self.db.references.find(query).limit(limit):
            result.append(reference)
        return self.generateJSON(metrics, result)

    def delRecords(self, query):
        '''Deletes records from MongoDB based upon the query passed.
        '''
        result=[]
        metrics={'total':self.db.references.find(query).count()}
        self.db.references.remove(query)
        return self.generateJSON(metrics,[])  
 
    def getChains(self, query={}, limit=0):
        '''
        Queries MongoDB and returns regexes based upon the query passed.
        '''
        result=[]
        metrics={'total':self.db.chains.find(query).count()}
        for reference in self.db.chains.find(query).limit(limit):
            result.append(reference)
        return self.generateJSON(metrics, result)

    def delChains(self, query={}):
        '''
        Deletes records from  MongoDB based upon the query passed.
        '''
        result=[]
        metrics={'total':self.db.chains.find(query).count()}
        self.db.chains.remove(query)
        return self.generateJSON(metrics, [])
        
    def insertChains(self, data):
        self.db.chains.insert(json.loads(data))
        return

class ReturnCodes():
    def __init__(self):
        self.template='<html><head>{0}</head><body><h1>{0}</h1></br>{1}</body></html>'
    
    def generateHeader(self, data):
        return [
            ('Content-Type', 'text/html'),
            ('Content-Length', str(len(data)))
        ]
            
    def code200(self, start_response, data):
        start_response('200 OK', self.generateHeader(data))
        return[data]
    
    def code204(self, start_response):
        start_response('204',self.generateHeader(''))
        return []

    def code400(self, start_response):
        message='400 Bad Request'
        start_response(message,self.generateHeader(message))
        return [message]

    def code404(self, start_response, data):
        start_response('404 Not Found', self.generateHeader(str(data)))
        return[str(data)]

    def code422(self, start_response, err):
        html = self.template.format('422 Unprocessable entity',err)
        start_response('422 Unprocessable entity', self.generateHeader(html))
        return [html]
        
class API_V1(ReturnCodes):
    def __init__(self, host='localhost', db='molog'):
        try:
            self.mongo = Connection(host)[db]
        except Exception as err:
            print sys.stderr.write('Could not connect to MongoDB. Reason: %s'%err)
            sys.exit(1)

        self.records=Records(self.mongo)
        self.chains=Chains(self.mongo)
        
    def help(self, sr, body, params, env):
        return self.code200(sr, "heeeeeeeeeeeeeeeeeelp")
        
class Records(ReturnCodes, MologTools):
    def __init__(self, mongodb):
        self.db=mongodb
        
    def GET(self, sr, body, params, env):
        try:
            if env.has_key('id'):
                #We're looking for a certain ID
                return self.code200(sr, self.getRecords(query={'_id':ObjectId(env['id'])}))
            else:
                #We're doing a query using searchparams if available.
                query = self.buildQuery(params,[ 'hostname', 'priority', 'tags' ])
                return self.code200(sr, self.getRecords(query, limit=int(params.get('limit',0))))
        except Exception as err:
            return self.code400(sr)
            
    def DELETE(self, sr, body, params, env):
        if env.has_key('id'):
            return self.code200(sr, self.delRecords(query={'_id':ObjectId(env['id'])}))
        else:
            query = self.buildQuery(params,[ 'hostname', 'priority', 'tags' ])
            return self.code200(sr, self.delRecords(query))

class Chains(ReturnCodes, MologTools):
    def __init__(self, mongodb):
        self.db=mongodb
        
    def GET(self, sr, body, params, env):
        try:
            if env.has_key('id'):
                #We're looking for a certain ID
                return self.code200(sr, self.getChains(query={'_id':ObjectId(env['id'])}))
            else:
                #We're doing a query using searchparams if available.
                query = self.buildQuery(params,[ 'name', 'tags' ])
                return self.code200(sr, self.getChains(query, limit=int(params.get('limit',0))))
        except Exception as err:
            return self.code400(sr)
    
    def POST(self, sr, body, params, env):
        if env.has_key('id'):
            #perform an update
            pass
        else:
            #add a record
            self.insertChains(body)
            return self.code200(sr, '' )

    def DELETE(self, sr, body, params, env):
        if env.has_key('id') and env.has_key('type') and env.has_key('index'):
            print "yeah"
        elif env.has_key('id'):
            return self.code200(sr, self.delRegexes(query={'_id':ObjectId(env['id'])}))
        else:
            query = self.buildQuery(params,[ 'name', 'tags' ])
            return self.code200(sr, self.delRegexes(query))

class Application(object):
    def __init__(self):
        self.rest = API_V1(host='sandbox', db='molog')
        self.answer = ReturnCodes()
        self.map = Mapper()
        self.map.connect('v1', '/v1', app=self.rest.help)
        self.map.connect('records', '/v1/records', app=self.rest.records.GET, conditions=dict(method=["GET"]))
        self.map.connect('records', '/v1/records', app=self.rest.records.DELETE, conditions=dict(method=["DELETE"]))
        self.map.connect('records', '/v1/records/{id}', app=self.rest.records.GET, conditions=dict(method=["GET"]))
        self.map.connect('records', '/v1/records/{id}', app=self.rest.records.DELETE, conditions=dict(method=["DELETE"]))
        
        self.map.connect('chains', '/v1/chains', app=self.rest.chains.GET, conditions=dict(method=["GET"]))
        self.map.connect('chains', '/v1/chains', app=self.rest.chains.POST, conditions=dict(method=["POST"]))
        self.map.connect('chains', '/v1/chains', app=self.rest.chains.DELETE, conditions=dict(method=["DELETE"]))

        self.map.connect('chains', '/v1/chains/{id}', app=self.rest.chains.GET, conditions=dict(method=["GET"]))
        #updating record is idempotent so PUT
        self.map.connect('chains', '/v1/chains/{id}', app=self.rest.chains.PUT, conditions=dict(method=["PUT"]))
        
        self.map.connect('chains', '/v1/chains/{id}', app=self.rest.chains.DELETE, conditions=dict(method=["DELETE"]))
        self.map.connect('chains', '/v1/chains/{id}/{type}/{index}', app=self.rest.chains.DELETE, conditions=dict(method=["DELETE"]))
        

    def genParameters(self, data):
        pass
    

    def __call__(self, environ, start_response):
        match = self.map.routematch(environ=environ)
        
        #Unknown urls are handled here
        if not match:
            return self.answer.code404(start_response, "No such url")

        #Verify the payload
        #try:
            #body = json.loads(''.join(environ['wsgi.input'].readlines()))
        #except Exception as err:
            #return self.rest.answer.return422(environ, start_response, err)
        
        #Serve content
        body = ''.join(environ['wsgi.input'].readlines())
        try:
            json.loads(body)
        except Exception as err:
            return self.rest.answer.return422(environ, start_response, err)
        
        url = urlparse.urlparse(environ['RAW_URI'])
        params = urlparse.parse_qs(url.query)
        for param in params:
            params[param]=params[param][0]
        return match[0]['app'](start_response, body, params, match[0])
        


app = Application()