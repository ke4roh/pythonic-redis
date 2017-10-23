"""Internal module for Python 2 backwards compatibility."""
import errno
import sys

try:
    InterruptedError = InterruptedError
except:
    InterruptedError = OSError

# For Python older than 3.5, retry EINTR.
if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and
                               sys.version_info[1] < 5):
    # Adapted from https://bugs.python.org/review/23863/patch/14532/54418
    import socket
    import time
    import errno

    from select import select as _select

    def select(rlist, wlist, xlist, timeout):
        while True:
            try:
                return _select(rlist, wlist, xlist, timeout)
            except InterruptedError as e:
                # Python 2 does not define InterruptedError, instead
                # try to catch an OSError with errno == EINTR == 4.
                if getattr(e, 'errno', None) == getattr(errno, 'EINTR', 4):
                    continue
                raise

    # Wrapper for handling interruptable system calls.
    def _retryable_call(s, func, *args, **kwargs):
        # Some modules (SSL) use the _fileobject wrapper directly and
        # implement a smaller portion of the socket interface, thus we
        # need to let them continue to do so.
        timeout, deadline = None, 0.0
        attempted = False
        try:
            timeout = s.gettimeout()
        except AttributeError:
            pass

        if timeout:
            deadline = time.time() + timeout

        try:
            while True:
                if attempted and timeout:
                    now = time.time()
                    if now >= deadline:
                        raise socket.error(errno.EWOULDBLOCK, "timed out")
                    else:
                        # Overwrite the timeout on the socket object
                        # to take into account elapsed time.
                        s.settimeout(deadline - now)
                try:
                    attempted = True
                    return func(*args, **kwargs)
                except socket.error as e:
                    if e.args[0] == errno.EINTR:
                        continue
                    raise
        finally:
            # Set the existing timeout back for future
            # calls.
            if timeout:
                s.settimeout(timeout)

    def recv(sock, *args, **kwargs):
        return _retryable_call(sock, sock.recv, *args, **kwargs)

    def recv_into(sock, *args, **kwargs):
        return _retryable_call(sock, sock.recv_into, *args, **kwargs)

else:  # Python 3.5 and above automatically retry EINTR
    from select import select

    def recv(sock, *args, **kwargs):
        return sock.recv(*args, **kwargs)

    def recv_into(sock, *args, **kwargs):
        return sock.recv_into(*args, **kwargs)

if sys.version_info[0] < 3:
    from urllib import unquote
    from urlparse import parse_qs, urlparse
    from itertools import imap, izip
    from string import letters as ascii_letters
    from Queue import Queue

    try:
        from cStringIO import StringIO as BytesIO
    except ImportError:
        from StringIO import StringIO as BytesIO

    # special unicode handling for python2 to avoid UnicodeDecodeError
    def safe_unicode(obj, *args):
        """ return the unicode representation of obj """
        try:
            return unicode(obj, *args)
        except UnicodeDecodeError:
            # obj is byte string
            ascii_text = str(obj).encode('string_escape')
            return unicode(ascii_text)

    def iteritems(x):
        return x.iteritems()

    def iterkeys(x):
        return x.iterkeys()

    def itervalues(x):
        return x.itervalues()

    def nativestr(x):
        return x if isinstance(x, str) else x.encode('utf-8', 'replace')

    def u(x):
        return x.decode()

    def b(x):
        return x

    def next(x):
        return x.next()

    def byte_to_chr(x):
        return x

    unichr = unichr
    xrange = xrange
    basestring = basestring
    unicode = unicode
    bytes = str
    long = long
else:
    from urllib.parse import parse_qs, unquote, urlparse
    from io import BytesIO
    from string import ascii_letters
    from queue import Queue

    def iteritems(x):
        return iter(x.items())

    def iterkeys(x):
        return iter(x.keys())

    def itervalues(x):
        return iter(x.values())

    def byte_to_chr(x):
        return chr(x)

    def nativestr(x):
        return x if isinstance(x, str) else x.decode('utf-8', 'replace')

    def u(x):
        return x

    def b(x):
        return x.encode('latin-1') if not isinstance(x, bytes) else x

    next = next
    unichr = chr
    imap = map
    izip = zip
    xrange = range
    basestring = str
    unicode = str
    safe_unicode = str
    bytes = bytes
    long = int

try:  # Python 3
    from queue import LifoQueue, Empty, Full
except ImportError:
    from Queue import Empty, Full

    try:  # Python 2.6 - 2.7
        from Queue import LifoQueue
    except ImportError:  # Python 2.5
        from Queue import Queue

        # From the Python 2.7 lib. Python 2.5 already extracted the core
        # methods to aid implementating different queue organisations.

        class LifoQueue(Queue):
            "Override queue methods to implement a last-in first-out queue."

            def _init(self, maxsize):
                self.maxsize = maxsize
                self.queue = []

            def _qsize(self, len=len):
                return len(self.queue)

            def _put(self, item):
                self.queue.append(item)

            def _get(self):
                return self.queue.pop()

try:
    from collections import OrderedDict
except ImportError:
    # https://github.com/ActiveState/code/blob/master/recipes/Python/107747_Ordered_Dictionary/recipe-107747.py
    #     Copyright (c) 2017 ActiveState Software Inc.
    #
    # Permission is hereby granted, free of charge, to any person obtaining a
    # copy of this software and associated documentation files (the
    # "Software"), to deal in the Software without restriction, including
    # without limitation the rights to use, copy, modify, merge, publish,
    # distribute, sublicense, and/or sell copies of the Software, and to
    # permit persons to whom the Software is furnished to do so, subject to
    # the following conditions:
    #
    # The above copyright notice and this permission notice shall be included
    # in all copies or substantial portions of the Software.
    #
    # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
    # OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
    # MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
    # IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
    # CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
    # TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
    # SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
    from UserDict import UserDict

    class OrderedDict(UserDict):
        def __init__(self, dict=None):
            self._keys = []
            UserDict.__init__(self, dict)

        def __delitem__(self, key):
            UserDict.__delitem__(self, key)
            self._keys.remove(key)

        def __setitem__(self, key, item):
            UserDict.__setitem__(self, key, item)
            if key not in self._keys:
                self._keys.append(key)

        def clear(self):
            UserDict.clear(self)
            self._keys = []

        def copy(self):
            dict = UserDict.copy(self)
            dict._keys = self._keys[:]
            return dict

        def items(self):
            return zip(self._keys, self.values())

        def keys(self):
            return self._keys

        def popitem(self):
            try:
                key = self._keys[-1]
            except IndexError:
                raise KeyError('dictionary is empty')

            val = self[key]
            del self[key]

            return (key, val)

        def setdefault(self, key, failobj=None):
            UserDict.setdefault(self, key, failobj)
            if key not in self._keys:
                self._keys.append(key)

        def update(self, dict):
            UserDict.update(self, dict)
            for key in dict.__iter__():
                if key not in self._keys:
                    self._keys.append(key)

        def values(self):
            return map(self.get, self._keys)

        def move_to_end(self, key):
            self._keys.remove(key)
            self._keys.append(key)
