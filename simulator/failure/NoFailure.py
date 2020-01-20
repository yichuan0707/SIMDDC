from numpy import Inf

from simulator.failure.EventGenerator import EventGenerator


class NoFailure(EventGenerator):

    def __init__(self, name, parameters):
        self.name = name

    def getName(self):
        return self.name

    def reset(self, current_time):
        return current_time

    def getCurrentTime(self):
        return Inf

    def generateNextEvent(self, current_time):
        return Inf
