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
    def generateJSON(self, object):
        data=[]
        for item in object:
            item['id']=str(item['_id'])
            del(item['_id'])
            data.append(item)
        return json.dumps(object)
    def getRecords(self, query, limit):
        '''Queries MongoDB and returns records based upon the query passed.
        '''
        result=[]
        for reference in self.db.references.find(query).limit(limit):
            result.append(reference)
        return self.generateJSON(result)
    def getRegexes(self, query, limit):
        '''Queries MongoDB and returns regexes based upon the query passed.
        '''
        for reference in self.db.chains.find(query).limit(limit):
            result.append(reference)
        return self.generateJSON(result)
class ReturnCodes():
    def __init__(self):
        self.template='<html><head>{0}</head><body><h1>{0}</h1></br>{1}</body></html>'
    
    def generateHeader(self, data):
        return [
            ('Content-Type', 'text/html'),
            ('Content-Length', str(len(data)))
        ]
        
    def code404(self, start_response, data):
        start_response('404 Not Found', self.generateHeader(data))
        return[data]

    def code422(self, start_response, err):
        html = self.template.format('422 Unprocessable entity',err)
        start_response('422 Unprocessable entity', self.generateHeader(html))
        return [html]
    
    def code200(self, start_response, data):
        start_response('200 OK', self.generateHeader(data))
        return[data]

class API_V1(ReturnCodes):
    def __init__(self, host='localhost', db='molog'):
        try:
            self.mongo = Connection(host)[db]
        except Exception as err:
            print sys.stderr.write('Could not connect to MongoDB. Reason: %s'%err)
            sys.exit(1)

        self.records=Records(self.mongo)
        
    def help(self, sr, body, params, env):
        return self.code200(sr, "heeeeeeeeeeeeeeeeeelp")
        
class Records(ReturnCodes, MologTools):
    def __init__(self, mongodb):
        self.db=mongodb
        
    def GET(self, sr, body, params, env):
        if env.has_key('id'):
            #We're looking for a certain ID
            return self.code200(sr, self.__getRecords(id=ObjectId(env['id'])))
        else:
            #We're doing a query using searchparams if available.
            query = self.buildQuery(params,[ 'hostname', 'priority', 'tags' ])
            return self.code200(sr, self.getRecords(query, limit=int(params.get('limit',0))))

    def POST(self, *args, **kwargs):
        return self.code200(args[0], "POST")

    def DELETE(self, *args, **kwargs):
        return self.code200(args[0], "DELETE")

class Regexes(ReturnCodes):
    def __init__(self, mongodb):
        self.mongodb=mongodb
        
    def GET(self, sr, body, params, env):
       if env.has_key('id'):
            #We're looking for a certain ID
            return self.code200(sr, self.__getRecords(id=ObjectId(env['id'])))
        else:
            #We're doing a query using searchparams if available.
            query = self.buildQuery(params,[ 'hostname', 'priority', 'tags' ])
            return self.code200(sr, self.getRecords(query, limit=int(params.get('limit',0))))

    def POST(self, *args, **kwargs):
        return self.code200(args[0], "POST")

    def DELETE(self, *args, **kwargs):
        return self.code200(args[0], "DELETE")        

class Totals(ReturnCodes):
    def __init__(self, mongodb):
        self.mongodb=mongodb
        
    def GET(self, sr, body, params):
        return self.code200(sr, str(params))

    def POST(self, *args, **kwargs):
        return self.code200(args[0], "POST")

    def DELETE(self, *args, **kwargs):
        return self.code200(args[0], "DELETE")                

class Application(object):
    def __init__(self):
        self.rest = API_V1(host='sandbox', db='molog')
        self.answer = ReturnCodes()
        self.map = Mapper()
        self.map.connect('v1', '/v1', app=self.rest.help)
        self.map.connect('records', '/v1/records', app=self.rest.records.GET, conditions=dict(method=["GET"]))
        self.map.connect('records', '/v1/records/{id}', app=self.rest.records.GET, conditions=dict(method=["GET"]))
        self.map.connect('records', '/v1/records/{id}', app=self.rest.records.POST, conditions=dict(method=["POST"]))
        self.map.connect('records', '/v1/records/{id}', app=self.rest.records.DELETE, conditions=dict(method=["DELETE"]))
        
        #self.map.connect('regexes', '/v1/regexes/{id}', app=self.rest.regexes)        
        #self.map.connect('totals', '/v1/totals/{id}', app=self.rest.totals)

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
        url = urlparse.urlparse(environ['RAW_URI'])
        params = urlparse.parse_qs(url.query)
        for param in params:
            params[param]=params[param][0]
        return match[0]['app'](start_response, body, params, match[0])
        


app = Application()
