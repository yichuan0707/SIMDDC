from math import exp, log
from numpy import isnan, isinf
from random import random

from simulator.failure.EventGenerator import EventGenerator


class WeibullGenerator(EventGenerator):
    """
    Weibull Distribution.
    """
    def __init__(self, name, parameters):
        self.name = name
        self.gamma = float(parameters['gamma'])
        self.lamda = float(parameters['lamda'])
        self.beta = float(parameters['beta'])
        self.start_time = 0

    def getName(self):
        return self.name

    def getCurrentTime(self):
        return self.start_time

    def reset(self, current_time):
        self.start_time = current_time

    def getRate(self):
        return self.lamda

    def F(self, current_time):
        return 1 - exp(-pow((current_time/self.lamda), self.beta))

    def generateNextEvent(self, current_time):
        current_time -= self.start_time
        if current_time < 0:
            raise Exception("Negative current time!")

        r = random()
        R = (1 - self.F(current_time)) * r + self.F(current_time)
        result = self.lamda*pow(-log(1.0-R), 1.0/self.beta) + \
            self.gamma+self.start_time

        if isinf(result) or isnan(result):
            raise Exception("Generated time is Inf or NaN")
        if result < 0:
            raise Exception("Generated time is negative")
        return result


def main():
    w = WeibullGenerator("wei", {'gamma': 0.02, 'lamda': 0.03, 'beta': 1})
    hist = {}
    for i in xrange(1000):
        next_event = w.generateNextEvent(0.0)
        next_event = float(next_event*10000.0)/10000.0
        if next_event in hist.keys():
            p = hist.get(next_event)
            hist[next_event] = p + 1.0
        else:
            hist[next_event] = 1.0

    for i in hist.items():
        print str(i[0]) + "  " + str(i[1])

def test():
    w = WeibullGenerator("wei", {'gamma': 0.0, 'lamda': 9259, 'beta': 1})
    current_time = 0.0
    TTF = []

    while current_time <= 87600.0:
        next_event = w.generateNextEvent(current_time)
        next_event = float(next_event*10000.0)/10000.0
        TTF.append(next_event - current_time)
        current_time = next_event

    TTF.sort()
    MTTF = sum(TTF)/len(TTF)
    print TTF
    print "MTTF:", MTTF
    return MTTF


if __name__ == "__main__":
    res = []
    for i in xrange(5000):
        res.append(test())
    print "MTTF for all:", sum(res)/len(res)
