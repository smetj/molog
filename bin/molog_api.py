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
    
    def generateJSON(self, result=[]):
        data=[]
        for item in result:
            if item.has_key('_id'):
                item['id']=str(item['_id'])
                del(item['_id'])
                data.append(item)
        return json.dumps(data)
        
    def insertChains(self, data):
        self.db.chains.insert(json.loads(data))
        return

    def overwriteChain(self, id, data):
        self.db.chains.update ({'_id':ObjectId(id)}, json.loads(data))
            
        
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
        self.db=mongodb['references']
    
    def getRecord(self, sr, body, params, env):
        query = {'_id':ObjectId(env['id'])}
        result = self.db.find(query)
        result = self.generateJSON(result)
        return self.code200(sr, result)
    
    def getRecords(self, sr, body, params, env):
        result=[]
        query = self.buildQuery(params,[ 'hostname', 'priority', 'tags' ])
        for reference in self.db.find(query).limit(params.get('limit',0)):
            result.append(reference)
        result = self.generateJSON(result)
        return self.code200(sr, result)
        
    def delRecord(self, sr, body, params, env):
        query = {'_id':ObjectId(env['id'])}
        self.db.remove(query)
        return self.code200(sr, '')
    
    def delRecords(self, sr, body, params, env):
        query = query = self.buildQuery(params,[ 'hostname', 'priority', 'tags' ])
        self.db.remove(query)
        return self.code200(sr, '')

class Chains(ReturnCodes, MologTools):
    def __init__(self, mongodb):
        self.db=mongodb['chains']
        
    def getChain(self, sr, body, params, env):
        
        query = {'_id':ObjectId(env['id'])}
        result = [self.db.chains.find(query)]
        result = self.generateJSON(result)
        return self.code200(sr, result)
    
    def getChains(self, sr, body, params, env):
        query = self.buildQuery(params,[ 'name', 'tags' ])
        result=[]

        for reference in self.db.find(query).limit(env.get('limit',0)):
            result.append(reference)
        result = self.generateJSON(result)
        return self.code200(sr, result)
    
    def POST(self, sr, body, params, env):
        self.insertChains(body)
        return self.code200(sr, '' )
    
    def overwriteTag(self, sr, body, params, env):
        tags = self.db.find_one({'_id':ObjectId(env['id'])},{'tags':1})['tags']
        tags[int(env['index'])]=body
        self.db.update ( {'_id':ObjectId(env['id'])}, {'$set':{'tags':tags}})
        return self.code200(sr, '')

    def overwriteTags(self, sr, body, params, env):
        self.db.update ( {'_id':ObjectId(env['id'])}, {'$set':{'tags':json.loads(body)}})
        return self.code200(sr, '')        
    
    def overwriteChain(self, sr, body, params, env):
        self.db.update ( {'_id':ObjectId(env['id'])}, {'$set':{'regexes':json.loads(body)}})
        return self.code200(sr, '')

    def overwriteChains(self, sr, body, params, env):
        self.db.remove({})
        self.db.update ( json.loads(body))
        return self.code200(sr, '')
        
    def overwriteRegex(self, sr, body, params, env):
        self.db.update ( {'_id':ObjectId(env['id'])}, {'$set':{'regexes.%s.regex'%(int(env['index'])):body}})
        return self.code200(sr, '')
    
    def overwriteField(self, sr, body, params, env):
        self.db.update ({'_id':ObjectId(env['id'])}, {'$set':{'regexes.%s.field'%(int(env['index'])):body}})
        return self.code200(sr, '')
        
    def overwriteType(self, sr, body, params, env):
        self.db.update ({'_id':ObjectId(env['id'])}, {'$set':{'regexes.%s.type'%(int(env['index'])):body}})
        return self.code200(sr, '')
        
    def delChain(self, sr, body, params, env):
        self.db.remove({'_id':ObjectId(env['id'])})
        return self.code200(sr, '')

    def delChains(self, sr, body, params, env):
        pass

    def delChainTag(self, sr, body, params, env):
        tags = self.db.find_one({'_id':ObjectId(env['id'])},{'tags':1})['tags']
        del tags[int(env['index'])]
        self.db.update ({'_id':ObjectId(env['id'])}, {'$set':{'tags':tags}})
        return self.code200(sr, '')
    
    def delChainRegex(self, sr, body, params, env):
        regexes = self.db.find_one({'_id':ObjectId(env['id'])},{'regexes':1})['regexes']
        del regexes[int(env['index'])]
        self.db.update ({'_id':ObjectId(env['id'])}, {'$set':{'regexes':regexes}})
        return self.code200(sr, '')
        
class Application(object):
    def __init__(self):
        self.rest = API_V1(host='sandbox', db='molog')
        self.answer = ReturnCodes()
        self.map = Mapper()
        self.map.connect('v1', '/v1', app=self.rest.help)

        #Records
        self.map.connect('records', '/v1/records/{id}', app=self.rest.records.getRecord, conditions=dict(method=["GET"]))
        self.map.connect('records', '/v1/records', app=self.rest.records.getRecords, conditions=dict(method=["GET"]))

        self.map.connect('records', '/v1/records/{id}', app=self.rest.records.delRecord, conditions=dict(method=["DELETE"]))
        self.map.connect('records', '/v1/records', app=self.rest.records.delRecords, conditions=dict(method=["DELETE"]))
        
        #Chains
        #Get the the content of one or more chais based on query
        self.map.connect('chains', '/v1/chains/{id}', app=self.rest.chains.getChain, conditions=dict(method=["GET"]))
        self.map.connect('chains', '/v1/chains', app=self.rest.chains.getChains, conditions=dict(method=["GET"]))
        
        #Insert a new chain data
        self.map.connect('chains', '/v1/chains', app=self.rest.chains.POST, conditions=dict(method=["POST"]))

        #Update an existing chain data (overwrite = idempotent)
        self.map.connect('chains', '/v1/chains', app=self.rest.chains.overwriteChains, conditions=dict(method=["PUT"]))
        self.map.connect('chains', '/v1/chains/{id}', app=self.rest.chains.overwriteChain, conditions=dict(method=["PUT"]))
        self.map.connect('chains', '/v1/chains/{id}/tags', app=self.rest.chains.overwriteTags, conditions=dict(method=["PUT"]))
        self.map.connect('chains', '/v1/chains/{id}/tags/{index}', app=self.rest.chains.overwriteTag, conditions=dict(method=["PUT"]))        
        self.map.connect('chains', '/v1/chains/{id}/regexes', app=self.rest.chains.overwriteChains, conditions=dict(method=["PUT"]))
        self.map.connect('chains', '/v1/chains/{id}/regexes/{index}', app=self.rest.chains.overwriteChain, conditions=dict(method=["PUT"]))
        self.map.connect('chains', '/v1/chains/{id}/regexes/{index}/regex', app=self.rest.chains.overwriteRegex, conditions=dict(method=["PUT"]))
        self.map.connect('chains', '/v1/chains/{id}/regexes/{index}/field', app=self.rest.chains.overwriteField, conditions=dict(method=["PUT"]))
        self.map.connect('chains', '/v1/chains/{id}/regexes/{index}/type', app=self.rest.chains.overwriteType, conditions=dict(method=["PUT"]))
        
        #Delete Chains
        self.map.connect('chains', '/v1/chains', app=self.rest.chains.delChains, conditions=dict(method=["DELETE"]))
        self.map.connect('chains', '/v1/chains/{id}', app=self.rest.chains.delChain, conditions=dict(method=["DELETE"]))
        self.map.connect('chains', '/v1/chains/{id}/regexes/{index}', app=self.rest.chains.delChainRegex, conditions=dict(method=["DELETE"]))
        self.map.connect('chains', '/v1/chains/{id}/tags/{index}', app=self.rest.chains.delChainTag, conditions=dict(method=["DELETE"]))

    def genParameters(self, data):
        pass

    def generateOutput(self, data):
        output=[]
        for item in data:
            if item.has_key('_id'):
                item['id']=str(item['_id'])
                del(item['_id'])
                output.append(item)
        return json.dumps(output)
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
        #try:
            #json.loads(body)
        #except Exception as err:
            #return self.rest.answer.return422(environ, start_response, err)
        
        url = urlparse.urlparse(environ['RAW_URI'])
        params = urlparse.parse_qs(url.query)
        for param in params:
            params[param]=params[param][0]
        return match[0]['app'](start_response, body, params, match[0])
        


app = Application()
