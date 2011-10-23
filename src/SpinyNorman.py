'''
Created on Oct 8, 2011

@author: emlyn
'''
import logging

from Worker import Worker
from datetime import timedelta
from google.appengine.ext import db
from time import sleep

class SpinyNorman(Worker):
    _minutesBetweenSpines = 12
    _spineWidth = 20
    _numberOfSpinesRemaining = db.IntegerProperty()
        
    def GetSpineLength(cls):
        return 1000
    GetSpineLength = classmethod(GetSpineLength)
    
    def CreateSpines(cls, aSpineLength, aNumberOfSpines):
        lcount = 0
        while lcount < aSpineLength:
            lnorman = SpinyNorman()
            lnorman._numberOfSpinesRemaining = aNumberOfSpines
            lnorman.enabled = True
            lnorman.put()
            lcount += 1
    CreateSpines = classmethod(CreateSpines)

    def doExecute(self):
        self._numberOfSpinesRemaining -= 1
        #
#        lcount = 0
#        while lcount < self._spineWidth:
#            lcount += 1
        logging.info("Begin sleep")
        sleep(self._spineWidth);
        logging.info("End sleep")
                
#        logging.debug(lcount)
    
    def doCalculateNextRun(self, aUtcNow, alastDue):
        if self._numberOfSpinesRemaining > 0:
            if alastDue:
                return alastDue + timedelta(minutes=self._minutesBetweenSpines)
            else:
                return aUtcNow + timedelta(minutes=self._minutesBetweenSpines)
        else:
            return None # time to stop
        
        