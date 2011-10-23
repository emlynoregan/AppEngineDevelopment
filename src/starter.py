import os
from google.appengine.ext.webapp import template
from google.appengine.ext import webapp
import logging
from SpinyNorman import SpinyNorman
import Semaphore
import DiningPhilosophers
 
class Starter(webapp.RequestHandler):
    
    def get(self):
        #SpinyNorman.CreateSpines()
        
        template_values = {}
        
        path = os.path.join(os.path.dirname(__file__), "starter.html")
        self.response.out.write(template.render(path, template_values))
        

    def post(self):
        try:
            lfunction = self.request.get("function", None)
            
            if lfunction == "Start Spiny Norman":
                SpinyNorman.CreateSpines(250, 100)
            elif lfunction == "Start SemaphoreTest1":
                Semaphore.SemaphoreTest1()
            elif lfunction == "Start DiningPhilosophersFail":
                DiningPhilosophers.DiningPhilosphersFailTest()
            elif lfunction == "Start DiningPhilosophersSucceed":
                DiningPhilosophers.DiningPhilosphersSucceedTest()
            else:
                logging.error("No such function %s" % lfunction)
        except Exception, ex:
            logging.error(ex)

        self.redirect(self.request.url)
    
