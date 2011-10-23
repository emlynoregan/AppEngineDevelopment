import webapp2

from starter import Starter

app = webapp2.WSGIApplication([('/starter', Starter)],
                              debug=True)
