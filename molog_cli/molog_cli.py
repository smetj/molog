import logging
import sys

sys.path.append("/opt/molog/lib/modules")
from cliff.app import App
from cliff.commandmanager import CommandManager
import mologcli

class MologCli(App):

    log = logging.getLogger(__name__)

    def __init__(self):        
        cm = CommandManager('molog')
        cm.add_command('records',mologcli.Records)
        cm.add_command('chains',mologcli.Chains)
        cm.add_command('error',mologcli.Error)
        super(MologCli, self).__init__(
            description='cliff demo app',
            version='0.1',
            command_manager=cm,
            )        

    def initialize_app(self, argv):
        self.log.debug('initialize_app')

    def prepare_to_run_command(self, cmd):
        self.log.debug('prepare_to_run_command %s', cmd.__class__.__name__)

    def clean_up(self, cmd, result, err):
        self.log.debug('clean_up %s', cmd.__class__.__name__)
        if err:
            self.log.debug('got an error: %s', err)


def main(argv=sys.argv[1:]):
    myapp = MologCli()
    return myapp.run(argv)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
