import logging
import sys

sys.path.append("/opt/molog/lib/modules")
from cliff.app import App
from cliff.commandmanager import CommandManager
import requests
import cmd
import argparse
from prettytable import PrettyTable
from pymongo import Connection

class Records(cmd.Cmd):
    
    prompt = "(molog:records) "
    
    def __init__(self, url):
        cmd.Cmd.__init__(self)
        self.url=url+"/records"

    def do_get(self, args):
        '''Allows you to retrieve the LogStash records which matched one or more of the defined chains:
        
        --id            The ID of the record.
        --hostname      Records with the provided hostname.
        --tags          Records containing the provided tag.
        --chain         Records with the provided priority.
        --message       Performa a search on the message using an ES text query.
        --limit         Limit the amount of records returned.
        
        Examples: 
            - Get the last 5 records
                (records) get --limit 5
                
            - Get details of 1 specific record
                (records) get --id 12345
                
            - Get the records with tag "nagiosWarning" for host "celest".
                (records) get --tags nagiosWarning --hostname celest
                
            - Get the records which contain test
                (records) get --message test
            
        
        '''
        table = PrettyTable(['ID','Tags','Chain'])
        table.add_column('Message',[], align='l')
        
        parser = argparse.ArgumentParser()
        parser.add_argument('--id',required=False)
        parser.add_argument('--logsource',required=False)
        parser.add_argument('--tags',required=False)
        parser.add_argument('--chain',required=False)
        parser.add_argument('--message',required=False)
        parser.add_argument('--limit',required=False)
        #ToDo(Jelle): implement pagination in api and deal with it instead of --limit (ES supports this.)
        
        try:
            a = vars(parser.parse_args(args.split()))
        except SystemExit as err:
            print "Invalid input: %s" % err
        else:
            #ToDo(Jelle): Ugly translate need to figure out something better here.
            a['@molog_tags']=a['tags']
            a['@molog_chain']=a['chain']
            a['@message']=a['message']
            if a['id'] != None:
                url = '%s/%s'%(self.url,a['id'])
            else:
                url = self.url
            r = requests.get(url,params=a)
            if r.json == None or len(r.json) == 0:
                print "Your query didn't return any matches."
                print "Answer status code: %s"%(r.status_code)
            else:
                for record in r.json:
                    table.add_row([record['@molog_id'],','.join(record['@molog_tags']),record['@molog_chain'],record['@message']])
                print table
                print "Answer status code: %s"%(r.status_code)

    def do_del(self, args):
        '''Allows you to delete MoLog registered LogStash records.
        
        Deleting is actually removing the @molog item from the document.
        Since ES 0.20 isn't out yet, we don't have yet support for update by query.
        As a consequence, deleting the references (actually updating the document) can take a long time.
            
            --id            The MoLog ID of the record.
            --logsource     Records with the provided hostname.
            --tags          Records containing the provided tag.
            --chain         Records with the provided priority.
            --message       Performa a search on the message using an ES text query.
            --limit         Define the amount of records. By default 100
            
            Examples:               
                - Delete 1 specific record
                    (records) delete --id 12345
        '''   

        parser = argparse.ArgumentParser()
        parser.add_argument('--id',required=False)
        parser.add_argument('--logsource',required=False)
        parser.add_argument('--tags',required=False)
        parser.add_argument('--chain',required=False)
        parser.add_argument('--message',required=False)
        parser.add_argument('--limit',required=False)
        
        try:
            a = vars(parser.parse_args(args.split()))
        except SystemExit as err:
            print "Invalid input: %s" % err
        else:
            #ToDo(Jelle): Ugly translate need to figure out something better here.
            a['@molog.tags']=a['tags']
            a['@molog.chain']=a['chain']
            a['@message']=a['message']
            
            if a['id'] != None:
                url = '%s/%s'%(self.url,a['id'])
            else:
                url = self.url
            
            r = requests.delete(url,params=a)
            print "Answer status code: %s"%(r.status_code)
           
    def do_quit(self, args):
        '''Exits the client.'''
        sys.exit(1)
    
    def do_exit(self, args):
        '''Goes back to the cli root.'''
        return True
    
    do_list = do_get

class Chains(cmd.Cmd):
    '''
    '''
    prompt = "(molog:chains) "

    def __init__(self, url):
        cmd.Cmd.__init__(self)
        self.url=url+"/chains"
        
    def do_list(self, args):
        '''Allows you to retrieve the LogStash chains:
        
        --id        The ID of the chain.      
        --name      The name of the chain.
        --tags      The tag(s) of the chain.
        '''
        
        table = PrettyTable()
        table.add_column('ID',[], align='l')
        table.add_column('Tags',[], align='l')
        table.add_column('Name',[], align='l')        
        
        parser = argparse.ArgumentParser()
        parser.add_argument('--id',required=False)
        parser.add_argument('--name',required=False)
        parser.add_argument('--tags',required=False)
        
        try:
            a = vars(parser.parse_args(args.split()))
        except SystemExit as err:
            print "Invalid input: %s" % err
        else:
            if a['id'] != None:
                url = '%s/%s'%(self.url,a['id'])
            else:
                url = self.url
            r = requests.get(url,params=a)
            if r.json == None or len(r.json) == 0:
                print "Your query didn't return any matches."
                print "Answer status code: %s"%(r.status_code)
            else:
                for record in r.json:
                    table.add_row([record['id'],','.join(record['tags']),record['name']])
                print table
                print "Answer status code: %s"%(r.status_code)

    def do_show(self, args):
        '''Shows the content of a chain.
        
        --id           The ID of the chain.
        --name          The name of the chain.
        '''
        
        def makeTable():
            table = PrettyTable()
            table.add_column('Index',[], align='l')
            table.add_column('Type',[], align='l')
            table.add_column('Field',[], align='l')
            table.add_column('Regex',[], align='l')
            return table
        
        parser = argparse.ArgumentParser()
        parser.add_argument('--id',required=False)
        parser.add_argument('--name',required=False)

        try:
            a = vars(parser.parse_args(args.split()))
        except SystemExit as err:
            print "Invalid input: %s" % err
        else:
            if a['id'] != None:
                url = '%s/%s'%(self.url,a['id'])
            else:
                url = self.url
            r = requests.get(url,params=a)
            if r.json == None or len(r.json) == 0:
                print "Your query didn't return any matches."
                print "Answer status code: %s"%(r.status_code)
            else:
                
                for chain in r.json:
                    table = makeTable()
                    counter=0
                    print chain['name']
                    print '-' * len (chain['name'])      
                    for regex in chain['regexes']:
                        table.add_row([counter,regex['type'],regex['field'],regex['regex']])
                        counter+=1
                    print table
                    print
                    
                print "Answer status code: %s"%(r.status_code)


    def do_insert(self, args):
        pass

    def do_update(self, args):
        pass

    def do_delete(self, args):
        pass     
           
    def do_quit(self, args):
        sys.exit(1)
    
    def do_exit(self, args):
        return True
    
    do_EOF = do_quit
    
class MologCli(cmd.Cmd):
    prompt = "(molog) "
    def __init__(self):
        cmd.Cmd.__init__(self)
        self.url='http://localhost:8000/v1'
        
    def do_records(self, args):
        '''
        Allows you to query, display and delete records which matched one of the Chains.
        '''
        
        sub_cmd = Records(self.url)
        sub_cmd.cmdloop()

    def do_chains(self, args):
        '''
        Allows you to query, display and manipulate Chains.
        '''
        
        sub_cmd = Chains(self.url)
        sub_cmd.cmdloop()
        
def main():
    MologCli().cmdloop()

if __name__ == '__main__':
    main()
