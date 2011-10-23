from google.appengine.ext import db
from google.appengine.ext.db import polymodel
from google.appengine.ext.db import NotSavedError

import logging

from datetime import datetime
from datetime import timedelta

from google.appengine.ext import deferred


import uuid

###
# Worker is designed as a base class for any persistent object which needs to execute in
# a background task. This can be once now, once in the future, or recurring.
###
class Worker(polymodel.PolyModel):
    nextDue = db.DateTimeProperty()
    
    enabled = db.BooleanProperty()
    status = db.IntegerProperty() # 0 = ready, 1 = running, 2 = stopped
    
    lastRunSucceeded = db.BooleanProperty()
    lastRunMessage = db.StringProperty() # only if
    lastRunStartTime = db.DateTimeProperty()
    lastRunFinishTime = db.DateTimeProperty()
    
    createTime = db.DateTimeProperty(auto_now_add = True)
    
    taskid = db.StringProperty()

    # override to change queues
    def GetQueue(self):
        return "default"

    # override to do first run in the future    
    def ExecuteImmediately(self):
        return True
    
    def Construct(cls, aQueue = None):
        lsw = cls()
        
        lsw.nextDue = None
        
        lsw.enabled = False
        lsw.status = 0
        
        lsw.lastRunSucceeded = None
        lsw.lastRunMessage = None
        lsw.lastRunStartTime = None
        lsw.lastRunFinishTime = None
        
        lsw.taskid = None
        
        lsw.put()
        
        return lsw
    Construct = classmethod(Construct)
    
    def doExecute(self):
        raise NotImplementedError
    
    def doCalculateNextRun(self, aUtcNow, alastDue):
        raise NotImplementedError
    
    def Execute(self, aSWTaskID, aIsFirstRun, **kwargs):
        logging.debug("Entered Execute for %s, %s, %s" % (self, aIsFirstRun, aSWTaskID))

        try:
            logging.debug("Don't trust depickled self, go reload self")
            self = db.get(self.key())
        except NotSavedError, ex:
            self = None
        
        lutcNow = datetime.utcnow()
        
        if not self:
            logging.warning("eek I am gone! (disappears in a puff of logic)")
        elif not aSWTaskID:
            logging.warning("No aSWTaskid, skipping")
        elif aSWTaskID != self.taskid:
            logging.debug("taskids do not match, skipping")
        elif not self.enabled:
            logging.warning("Disabled, skipping")
        elif self.status != 0:
            logging.warning("Wrong status to execute Scheduled Worker, status = %s, skipping" % (self.status))
        elif self.nextDue and self.nextDue > lutcNow:
            logging.debug("Don't run till %s, reschedule..." % (self.nextDue))
            if (self.nextDue - lutcNow) > timedelta(1):
                lresched = lutcNow + timedelta(1) # add a day
            else:
                lresched = self.nextDue

            lqueue = self.GetQueue()
            
            deferred.defer(self.Execute, _queue_name=lqueue, _eta=lresched, aSWTaskID=self.taskid, aIsFirstRun=aIsFirstRun)
        else:
            if aIsFirstRun and not self.nextDue and not self.ExecuteImmediately():
                logging.debug("First run, don't execute")
            else:
                logging.debug("We can execute")
                try:
                    self.status = 1
                    self.lastRunStartTime = datetime.utcnow()
                    self.put()
    
                    logging.debug("Before Execute")
                    self.doExecute()
                    logging.debug("After Execute")
                    
                    self.status = 0
                    self.lastRunSucceeded = True
                    self.lastRunMessage = None
                except Exception, ex:
                    self.status = 0
                    self.lastRunSucceeded = False
                    self.lastRunMessage = unicode(ex)
                    logging.error(ex)

                self.lastRunFinishTime = datetime.utcnow()
                self.put()

            logging.debug("calculate lnextRun")
            lnaiveUtcNow = datetime.utcnow()

            lutcnow = lnaiveUtcNow
            
            lnextRun = self.doCalculateNextRun(lutcnow, self.nextDue )
            
            if lnextRun:
                logging.debug("got lnextRun, need to reschedule")
                self.nextDue = lnextRun
                self.status = 0
                self.put()
            
                lqueue = self.GetQueue()
                
                if (lnextRun - lutcnow) > timedelta(1):
                    lresched = lutcnow + timedelta(1)
                else:
                    lresched = lnextRun
                    
                if lresched <= lutcnow:
                    # run immediately
                    deferred.defer(self.Execute, _queue_name=lqueue, aSWTaskID=self.taskid, aIsFirstRun=False)
                else:
                    # schedule future run
                    deferred.defer(self.Execute, _queue_name=lqueue, _eta=lresched, aSWTaskID=self.taskid, aIsFirstRun=False)
            else:
                logging.debug("no lnextRun, we are finished.")
                self.status = 2
                self.put()

    def put(self, **kwargs):
        lneedPut = True

        # first grab a copy of what's currently stored.
        logging.debug("Entered put, new self = %s" % (self))

        loldself = None
        if self.enabled:
            logging.debug("Need to find out if enabled has been newly set. Load old self from datastore")
    
            try:
                loldself = self.get(self.key())
                logging.debug("Old self from datastore = %s" % (loldself))
            except Exception, ex:
                logging.debug(ex)
                loldself = None
    
        logging.debug("See if newly enabled has changed")
        if self.enabled and (not loldself or not loldself.enabled):
            logging.debug("Newly enabled. Need to schedule self to run")

            self.taskid = unicode(uuid.uuid4())
            logging.debug("sw-taskid == %s" % (self.taskid))

            logging.debug("Now schedule to run immediately")
    
            #self.nextDue = None
            self.status = 0
            
            logging.debug("Pre-save")
            super(Worker, self).put(**kwargs)
            lneedPut = False
        
            lqueue = self.GetQueue()

            # run immediately                
            logging.debug("call deferred.defer")
            deferred.defer(self.Execute, _queue_name=lqueue, aSWTaskID=self.taskid, aIsFirstRun=True)
        else:
            logging.debug("not newly enabled")

        if lneedPut:
            # now actually save to the db
            logging.debug("Do the actual put")
            super(Worker, self).put(**kwargs)


class ImmediateWorker(Worker):
    
    def doCalculateNextRun(self, aUtcNow, alastDue):
        return None
    

    
