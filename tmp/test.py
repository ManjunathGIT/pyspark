print type("a")

print type(u"a")

print isinstance("a", basestring)

print isinstance(u"a", basestring)

print type(u"a".encode("utf-8") == "a")

print type(bytearray(u"a".encode("utf-8")))

val = bytearray('abcefg')

print type(val[0])

print 2**8

import sys

print -sys.maxint - 1
