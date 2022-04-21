from enum import Enum
import  json


class Command(Enum):
    ONBOARD = 'ONBOARD'
    START = 'START'
    MONITOR = 'MONITOR'
    
    @classmethod
    def has_value(cls, value):
        return value.name in cls._value2member_map_.keys()

class CommandFactory(object):
    def __init__(self, command: Command):
        self.command = command.name
        if not Command.has_value(command):
            raise ValueError(f"{command} is not a member of ConnectionType class.")
        if self.command == Command.ONBOARD:
            self.command_handler = self.onboard
        elif self.command == Command.START:
            self.command_handler = self.onboard
            

    def handler(self, fn_handler):
        self.command_handler = fn_handler
        return self

    def execute(self, **kwargs):
        self.command_handler(kwargs)
