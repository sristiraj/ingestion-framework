from abc import ABC, abstractmethod
from typing import *
from logging import Logger


class BaseDAO(ABC):
    
    @abstractmethod
    def create_record(self, record: TypedDict) -> bool:
        pass
    
    @abstractmethod
    def read_record(self, key: str):
        pass

