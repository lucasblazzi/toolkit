from abc import ABC, abstractmethod


class LogHandler(ABC):

    def __init__(self, successor=None):
        self._successor = successor

    @abstractmethod
    def handle(self, level: int, message: str):
        pass


class PowerToolsLogger(LogHandler):

    def handle(self, level: int, message: str):
        if level <= 2:
            print("Console:", message)
        elif self._successor:
            self._successor.handle(level, message)


class TeamsLogger(LogHandler):

    def handle(self, level: int, message: str):
        if level <= 5:
            print("File:", message)
        elif self._successor:
            self._successor.handle(level, message)


class EmailLogger(LogHandler):

    def handle(self, level: int, message: str):
        if level <= 5:
            print("File:", message)
        elif self._successor:
            self._successor.handle(level, message)


class PhoneLogger(LogHandler):

    def handle(self, level: int, message: str):
        if level <= 5:
            print("File:", message)
        elif self._successor:
            self._successor.handle(level, message)


logger = PowerToolsLogger(TeamsLogger(EmailLogger(PhoneLogger())))
logger.handle(1, "Critical system failure!")
