from google.appengine.ext import db
from google.appengine.ext.db import polymodel
from google.appengine.ext.db import TransactionFailedError
import logging
from google.appengine.ext import deferred
from time import sleep
import base64
import pickle

class Semaphore(polymodel.PolyModel):
    _counter = db.IntegerProperty()
    _suspendList = db.StringListProperty()

    def ConstructSemaphore(cls, aCounter):
        retval = cls()
        retval._counter = aCounter
        retval._suspendList = []
        return retval
    ConstructSemaphore = classmethod(ConstructSemaphore)
    
    def Wait(self, obj, *args, **kwargs):
        while True:
            try:
                lneedsRun = db.run_in_transaction(
                        _doWait, 
                        self.key(), 
                        obj, *args, **kwargs
                )
                if lneedsRun:
                    try:
                        obj(*args, **kwargs)
                    except Exception, ex:
                        logging.error(ex)
                break
            except TransactionFailedError:
                # do nothing
                logging.warning("TransactionFailedError in Wait, try again")

    def Signal(self):
        while True:
            try:
                db.run_in_transaction(_doSignal, self.key())
                break
            except TransactionFailedError:
                # do nothing
                logging.warning("TransactionFailedError in Signal, try again")
              
def _doWait(aKey, aObj, *args, **kwargs):
    lneedsRun = False
    
    lsem = db.get(aKey)
    if not lsem:
        raise Exception("Internal: failed to retrieve semaphore in _doWait")
  
    if lsem._counter > 0:
        lsem._counter -= 1
        logging.debug("counter: %s" % lsem._counter)
        lneedsRun = True
    else:
        logging.debug("about to defer")
        pickled = deferred.serialize(aObj, *args, **kwargs)
        pickled = base64.encodestring(pickled)
        logging.debug("after defer, pickled=%s" % pickled)
        lsem._suspendList.append(pickled)
    lsem.put()
    return lneedsRun

def _doSignal(aKey):
    lsem = db.get(aKey)
    if not lsem:
        raise Exception("Internal: failed to retrieve semaphore in _doSignal")

    if len(lsem._suspendList) > 0:
        logging.debug("about to unsuspend")
        pickled = lsem._suspendList.pop()
        pickled = base64.decodestring(pickled)
        #pickled = urllib.unquote(pickled)
        logging.debug("pickled=%s" % pickled)
        try:
            obj, args, kwds = pickle.loads(pickled)
        except Exception, e:
            raise deferred.PermanentTaskFailure(e)
        
        logging.debug("about to defer")
        deferred.defer(obj, _transactional=True, *args, **kwds)
        #deferred.run(pickled)
        logging.debug("after defer")
    else:
        lsem._counter += 1
    lsem.put()

def SemaphoreTest1():
    logging.info("*****************************")
    logging.info("**   BEGIN SEMAPHORETEST1  **")
    logging.info("*****************************")
    lsem = Semaphore.ConstructSemaphore(2)
    lsem.put()
    
    lcount = 0
    
    while lcount < 20:
        deferred.defer(SemaphoreTest1EntryPoint, lsem.key(), lcount, True)
        lcount += 1
        
def SemaphoreTest1EntryPoint(aKey, aNum, aFirst):
    lsem = db.get(aKey)
    if not lsem:
        raise Exception("Failed to retrieve semaphore in EntryPoint1")

    if aFirst:
        # this is before we've got the semaphore
        logging.info("Before Wait for %s" % aNum)
        lsem.Wait(SemaphoreTest1EntryPoint, aKey, aNum, False)
    else:
        # we now have the semaphore
        logging.info("Begin Critsec for %s" % aNum)
        sleep(2) # stay inside critsec for 2 seconds
        logging.info("End Critsec for %s" % aNum)
        lsem.Signal()
        logging.info ("After Signal for %s" % aNum)
    
