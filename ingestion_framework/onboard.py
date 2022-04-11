import os, sys
sys.path.append(os.path.dirname(__file__))

from util.command import CommandFactory
from util.command import Command
from core.dao import *


def onboard_dataset():
    pass

def trigger(payload: dict):
    
    cf = CommandFactory(Command.ONBOARD).handler(onboard_dataset)
    response = cf.execute()

    