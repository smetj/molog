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
    prompt = "(records) "
    
    def __init__(self, url):
        cmd.Cmd.__init__(self)
        self.url=url+"/records"

    def do_get(self, args):
        '''Allows you to retrieve the LogStash records which matched one or more of the defined chains:
        
        --id            The MoLog ID of the record.
        --hostname      Records with the provided hostname.
        --tags          Records containing the provided tag.
        --chain         Records with the provided priority.
        --message       Performa a search on the message using an ES text query.
        --limit         Limit the amount of records returned.
        --all           Includes all columns.
        
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
            a['@molog.tags']=a['tags']
            a['@molog.chain']=a['chain']
            a['@message']=a['message']
            if a['id'] != None:
                url = '%s/%s'%(self.url,a['id'])
            else:
                url = self.url
            r = requests.get(url,params=a)
            if len(r.json) == 0:
                print "Your query didn't return any matches."
            else:
                for record in r.json:
                    table.add_row([record['@molog']['id'],','.join(record['@molog']['tags']),record['@molog']['chain'],record['@message']])
                print table
        

    def do_del(self, args):
        '''Allows you to delete MoLog registered LogStash records:
            
            --id            The MoLog ID of the record.
            --logsource     Records with the provided hostname.
            --tags          Records containing the provided tag.
            --chain         Records with the provided priority.
            --message       Performa a search on the message using an ES text query.
            
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
        return True
    do_EOF = do_quit

class Chains(cmd.Cmd):
    prompt = "(chains) "

    def do_get(self, args):
        pass

    def do_insert(self, args):
        pass

    def do_update(self, args):
        pass

    def do_delete(self, args):
        pass     
           
    def do_quit(self, args):
        return True
    do_EOF = do_quit
    
class MologCli(cmd.Cmd):
    
    def __init__(self):
        cmd.Cmd.__init__(self)
        self.url='http://localhost:8000/v1'
        
    def do_records(self, args):
        sub_cmd = Records(self.url)
        sub_cmd.cmdloop()

    def do_chains(self, args):
        sub_cmd = Chains()
        sub_cmd.cmdloop()
        
def main():
    MologCli().cmdloop()

if __name__ == '__main__':
    main()
