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
#   $ gunicorn molog_api:molog


try:
    from routes import Mapper
except:
    print "This example requires Routes to be installed"
import json
import sys
import inspect
import urlparse
import pyes
from pymongo import Connection
import pyes
#from pyes import ES, q
#from pyes import query as esquery
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

class ReturnCodes():
 
    def __init__(self):
        self.template='<html><head>{0}</head><body><h1>{0}</h1></br>{1}</body></html>'
    
    def generateHeader(self, data):
        return [
            ('Content-Type', 'application/json'),
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

    def __init__(self, mongohost='localhost', mongodb='molog', eshost='localhost'):
        try:
            self.mongo = Connection(mongohost)[mongodb]
            self.es = pyes.ES("%s:9200"%(eshost))
        except Exception as err:
            print sys.stderr.write('Could not connect to MongoDB. Reason: %s'%err)
            sys.exit(1)

        self.records=Records(self.mongo, self.es)
        self.chains=Chains(self.mongo)
        self.debug=Debug(self.mongo)
        
    def help(self, sr, body, params, env):
        return self.code200(sr, "heeeeeeeeeeeeeeeeeelp")

class Debug(ReturnCodes, MologTools):
    
    def __init__(self, mongodb):
        self.db=mongodb
            
    def get(self, sr, body, params, env):
        print "Method: %s\nParams:\n%s\n"%("GET", params)
        return self.code200(sr, '')

    def put(self, sr, body, params, env):
        print "Method: %s\nParams:\n%s\nBody:\n%s\n"%("PUT", params, body)
        return self.code200(sr, '')
        
    def post(self, sr, body, params, env):
        print "Method: %s\nParams:\n%s\nBody:\n%s\n"%("POST", params, body)
        return self.code200(sr, '')
        
    def delete(self, sr, body, params, env):
        print "Method: %s\nParams:\n%s\nBody:\n%s\n"%("DELETE", params, body)
        return self.code200(sr, '')
                
class Records(ReturnCodes, MologTools):

    def __init__(self, mongodb, es):
        self.db=mongodb['references']
        self.es=es
    
    def queryBuilder(self, params):
        query = pyes.query.BoolQuery()
        for item in [ 'logsource', '@molog.chain', '@molog.tags' ]:
            if params.get(item,None) != None:
                query.add_must(pyes.query.TextQuery(item,params[item]))
        filter = pyes.filters.BoolFilter()
        filter.add_must(pyes.filters.LimitFilter(params.get('limit',100)))
        filter.add_must(pyes.filters.ExistsFilter('@molog.chain'))
        return pyes.query.FilteredQuery(query, filter)
            
    def getRecord(self, sr, body, params, env):
        search = pyes.query.IdsQuery(env['id'])
        result = json.dumps(self.es.search(query=search)[0:])
        return self.code200(sr, result) 
    
    def getRecords(self, sr, body, params, env):
        result=[]
        q = self.queryBuilder(params)
        for reference in self.es.search(query=q,scan=True):
            reference['@molog']['id']=reference._meta.id
            result.append(reference)
        return self.code200(sr, json.dumps(result))
        
    def delRecord(self, sr, body, params, env):
        query = {'_id':ObjectId(env['id'])}
        self.db.remove(query)
        return self.code200(sr, '')
    
    def delRecords(self, sr, body, params, env):
        query = query = self.buildQuery(params,[ 'hostname', 'priority', 'tags' ])
        self.db.remove(query)
        return self.code200(sr, '')

    def getEsMessage(self, id):
        return self.es.search(esquery.IdsQuery(id))[0]['@message']

class Chains(ReturnCodes, MologTools):

    def __init__(self, mongodb):
        self.db=mongodb['chains']
        
    def getChain(self, sr, body, params, env):        
        query = {'_id':ObjectId(env['id'])}
        
        result = [self.db.find_one(query)]
        result = self.generateJSON(result)
        return self.code200(sr, result)
    
    def getChains(self, sr, body, params, env):
        query = self.buildQuery(params,[ 'name', 'tags' ])
        result=[]

        for reference in self.db.find(query).limit(env.get('limit',0)):
            result.append(reference)
        result = self.generateJSON(result)
        return self.code200(sr, result)
    
    def insertChains(self, sr, body, params, env):
        print body
        self.db.insert(json.loads(body))
        return self.code200(sr,'')
    
    def overwriteName(self, sr, body, params, env):
        self.db.update ( {'_id':ObjectId(env['id'])}, {'$set':{'name':body}})
        return self.code200(sr, '')
    
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
        query = self.buildQuery(params,[ 'name', 'tags' ])
        self.db.remove(query)
        return self.code200(sr, '')

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
        self.rest = API_V1(mongohost='sandbox', mongodb='molog', eshost='sandbox')
        self.answer = ReturnCodes()
        self.map = Mapper()
        self.map.connect('v1', '/v1', app=self.rest.help)

        #Read Records
        self.map.connect('records', '/v1/records', app=self.rest.records.getRecords, conditions=dict(method=["GET"]))
        self.map.connect('records', '/v1/records/{id}', app=self.rest.records.getRecord, conditions=dict(method=["GET"]))
        
        #Delete Records
        self.map.connect('records', '/v1/records', app=self.rest.records.delRecords, conditions=dict(method=["DELETE"]))
        self.map.connect('records', '/v1/records/{id}', app=self.rest.records.delRecord, conditions=dict(method=["DELETE"]))
        
        #Read Chains
        self.map.connect('chains', '/v1/chains', app=self.rest.chains.getChains, conditions=dict(method=["GET"]))
        self.map.connect('chains', '/v1/chains/{id}', app=self.rest.chains.getChain, conditions=dict(method=["GET"]))

        #Delete Chains
        self.map.connect('chains', '/v1/chains', app=self.rest.chains.delChains, conditions=dict(method=["DELETE"]))
        self.map.connect('chains', '/v1/chains/{id}', app=self.rest.chains.delChain, conditions=dict(method=["DELETE"]))
        self.map.connect('chains', '/v1/chains/{id}/regexes/{index}', app=self.rest.chains.delChainRegex, conditions=dict(method=["DELETE"]))
        self.map.connect('chains', '/v1/chains/{id}/tags/{index}', app=self.rest.chains.delChainTag, conditions=dict(method=["DELETE"]))
        
        #Create new entry in collection
        self.map.connect('chains', '/v1/chains', app=self.rest.chains.insertChains, conditions=dict(method=["POST"]))
        #Replace the entire collection
        self.map.connect('chains', '/v1/chains', app=self.rest.chains.overwriteChains, conditions=dict(method=["PUT"]))
        #Replace the entire collection item (you can not create a new collection item by providing an ID since the ID is autogenerated by MongoDB)
        self.map.connect('chains', '/v1/chains/{id}', app=self.rest.chains.overwriteChain, conditions=dict(method=["PUT"]))
        
        
        self.map.connect('chains', '/v1/chains/{id}/name', app=self.rest.chains.overwriteName, conditions=dict(method=["PUT"]))
        self.map.connect('chains', '/v1/chains/{id}/tags', app=self.rest.chains.overwriteTags, conditions=dict(method=["PUT"]))
        self.map.connect('chains', '/v1/chains/{id}/tags/{index}', app=self.rest.chains.overwriteTag, conditions=dict(method=["PUT"]))
        self.map.connect('chains', '/v1/chains/{id}/regexes', app=self.rest.chains.overwriteChains, conditions=dict(method=["PUT"]))
        self.map.connect('chains', '/v1/chains/{id}/regexes/{index}', app=self.rest.chains.overwriteChain, conditions=dict(method=["PUT"]))
        self.map.connect('chains', '/v1/chains/{id}/regexes/{index}/regex', app=self.rest.chains.overwriteRegex, conditions=dict(method=["PUT"]))
        self.map.connect('chains', '/v1/chains/{id}/regexes/{index}/field', app=self.rest.chains.overwriteField, conditions=dict(method=["PUT"]))
        self.map.connect('chains', '/v1/chains/{id}/regexes/{index}/type', app=self.rest.chains.overwriteType, conditions=dict(method=["PUT"]))
        
        self.map.connect('debug', '/v1/debug', app=self.rest.debug.get, conditions=dict(method=["GET"]))
        self.map.connect('debug', '/v1/debug', app=self.rest.debug.put, conditions=dict(method=["PUT"]))
        self.map.connect('debug', '/v1/debug', app=self.rest.debug.post, conditions=dict(method=["POST"]))
        self.map.connect('debug', '/v1/debug', app=self.rest.debug.delete, conditions=dict(method=["DELETE"]))
        
    def __call__(self, environ, start_response):
        match = self.map.routematch(environ=environ)
        
        #Unknown urls are handled here
        if not match:
            return self.answer.code404(start_response, "No such url")
        else:
            body = ''.join(environ['wsgi.input'].readlines())
            url = urlparse.urlparse(environ['RAW_URI'])
            params = urlparse.parse_qs(url.query)
            for param in params:
                params[param]=params[param][0]
            return match[0]['app'](start_response, body, params, match[0])
            

molog = Application()
