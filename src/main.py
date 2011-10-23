from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app

from starter import Starter

WSGIApplicationArray =     [
     ('/', Starter), 
     ('/starter', Starter) 
    ]

application = webapp.WSGIApplication(
    WSGIApplicationArray,
    debug=True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()