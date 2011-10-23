from google.appengine.ext import db
import logging
from google.appengine.ext import deferred
from time import sleep
import datetime

import Semaphore

class Fork(Semaphore.Semaphore):
    def ConstructFork(cls):
        lfork = cls.ConstructSemaphore(1)
        return lfork
    ConstructFork = classmethod(ConstructFork)

def EatByKey(aFirstForkKey, aSecondForkKey, aIndex, aHasFirst, aHasSecond):
    lFirstFork = db.get(aFirstForkKey)
    if not lFirstFork:
        raise Exception("Failed to retrieve Left Fork")
    lSecondFork = db.get(aSecondForkKey)
    if not lSecondFork:
        raise Exception("Failed to retrieve Right Fork")
    Eat(lFirstFork, lSecondFork, aIndex, aHasFirst, aHasSecond)
    
def Eat(aFirstFork, aSecondFork, aIndex, aHasFirst=False, aHasSecond=False):
    if not aHasFirst:
        # this is before we've got the semaphore
        logging.info("Wait on first for %s" % aIndex)
        aFirstFork.Wait(EatByKey, aFirstFork.key(), aSecondFork.key(), aIndex, True, False)
    elif not aHasSecond:
        sleep(10) # takes a while to pick up the second fork!
        logging.info("Wait on second for %s" % aIndex)
        aSecondFork.Wait(EatByKey, aFirstFork.key(), aSecondFork.key(), aIndex, True, True)
    else:
        logging.info("EAT for %s" % aIndex)
        logging.info("Dropping second fork for %s" % aIndex)
        aSecondFork.Signal()
        logging.info("Dropping first fork for %s" % aIndex)
        aFirstFork.Signal()

def DiningPhilosphersFailTest():
    lnumPhilosophers = 5
    leta = datetime.datetime.utcnow() + datetime.timedelta(seconds=20)
    
    lforks = []
    lforkIndex = 0
    while lforkIndex < lnumPhilosophers:
        lfork = Fork.ConstructFork()
        lfork.put()
        lforks.append(lfork)
        lforkIndex += 1
    
    lphilosopherIndex = 0
    while lphilosopherIndex < lnumPhilosophers:
        deferred.defer(
            Eat, 
            lforks[lphilosopherIndex], 
            lforks[(lphilosopherIndex+1) % lnumPhilosophers],
            lphilosopherIndex,
            _eta = leta
            )
        lphilosopherIndex += 1
                                
def DiningPhilosphersSucceedTest():
    lnumPhilosophers = 5
    leta = datetime.datetime.utcnow() + datetime.timedelta(seconds=20)
    
    lforks = []
    lforkIndex = 0
    while lforkIndex < lnumPhilosophers:
        lfork = Fork.ConstructFork()
        lfork.put()
        lforks.append(lfork)
        lforkIndex += 1
    
    lphilosopherIndex = 0
    while lphilosopherIndex < lnumPhilosophers:
        if lphilosopherIndex < lnumPhilosophers-1:
            # not the last one
            deferred.defer(
                Eat, 
                lforks[lphilosopherIndex], 
                lforks[lphilosopherIndex+1],
                lphilosopherIndex,
                _eta = leta
                )
                
        else:
            # the last one
            deferred.defer(
                Eat,
                lforks[0],
                lforks[lphilosopherIndex],
                lphilosopherIndex,
                _eta = leta
                );
        lphilosopherIndex += 1
                