#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  records.py
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

import logging
import restclient
from cliff.command import Command


class GetRecords(Command):
    "A command which allows you to manipulate records."    
    log = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(GetRecords, self).get_parser(prog_name)
        parser.add_argument('command', nargs=1, default='')
        parser.add_argument('--id', nargs=1, default='')
        parser.add_argument('--hostname', nargs=1, default='')
        parser.add_argument('--priority', nargs=1, default='')
        parser.add_argument('--tags', nargs=1, default='')
        parser.add_argument('--limit', nargs=1, default='')
        return parser    

    def take_action(self, parsed_args):
        args=vars(parsed_args)
        #self.app.stdout.write('yaaaaaaaaaaaaaaaaaay\n')

class DeleteRecords(Command):
    "A command which allows you to manipulate records."    
    log = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(DeleteRecords, self).get_parser(prog_name)
        parser.add_argument('command', nargs=1, default='')
        parser.add_argument('--id', nargs=1, default='')
        parser.add_argument('--hostname', nargs=1, default='')
        parser.add_argument('--priority', nargs=1, default='')
        parser.add_argument('--tags', nargs=1, default='')
        parser.add_argument('--limit', nargs=1, default='')
        return parser    

    def take_action(self, parsed_args):
        args=vars(parsed_args)
        #self.app.stdout.write('yaaaaaaaaaaaaaaaaaay\n')
            

class Error(Command):
    "Always raises an error"

    log = logging.getLogger(__name__)

    def take_action(self, parsed_args):
        self.log.info('causing error')
        raise RuntimeError('this is the expected exception')
