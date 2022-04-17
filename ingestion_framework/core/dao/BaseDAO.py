from abc import ABC, abstractmethod
from typing import *
from logging import Logger
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from core.connection import Connection

class BaseDAO(ABC):
    
    @abstractmethod
    def create_record(self, record: TypedDict, connector: Connection) -> bool:
        pass
    
    @abstractmethod
    def read_record(self, record: Dict,  connector: Connection):
        pass

    @abstractmethod
    def update_record(self, record: Dict, connector: Connection):
        pass

