"""
Base Handler
SPDX-License-Identifier: LGPL-3.0-or-later
"""
import logging
from abc import ABC, abstractmethod

class EventHandler(ABC):
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def get_event_type(self) -> str:
        pass
    
    @abstractmethod
    def handle(self, event_data: dict) -> None:
        pass
