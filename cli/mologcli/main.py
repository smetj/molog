import logging
import sys

sys.path.append("/opt/molog/lib/modules")
from cliff.app import App
from cliff.commandmanager import CommandManager
import requests
import cmd
from prettytable import PrettyTable
from pymongo import Connection

class Records(cmd.Cmd):
    prompt = "(records) "
    
    def __init__(self, url):
        cmd.Cmd.__init__(self)
        self.url=url+"/records"

    def do_get(self, args):
        '''Allows you to retrieve records from MoLog using following search parameters:
        
        --id            The MoLog ID of the record.
        --hostname      Records with the provided hostname
        --tags          Records containing the provided tag
        --priority      Records with the provided priority
        
        get --id 12345
        
        '''
        r = requests.get(self.url)
        table = PrettyTable(['Timestamp','ID','Hostname','Tags','Chain','Message'])
        for record in r.json:
            table.add_row([record['timestamp'],record['id'],record['hostname'],','.join(record['tags']),record['chain'],record['es_id']])
        print table
        

    def do_delete(self, args):
        pass     
           
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
