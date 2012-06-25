#!/usr/bin/python

from pymongo import Connection

connection = Connection( 'sandbox', 27017 )
connection.molog.chains.remove({})

connection.molog.chains.insert({'regexes': [{'regex': '(1|2|3|4)', 'field': 'priority', 'type': 'positive'}], 'name': 'standardLinux', 'tags': ['linux']})

