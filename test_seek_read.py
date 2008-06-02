import unittest
import tempfile
import os
import sys

from seek_read import seek_open
from utils import file_offsets

class SmartReadTestCase(unittest.TestCase):
    def setUp(self):
        testdata = "12345678\nab\n3456789\ncdef\n87654\npqrstuvw\n"
        
        (handle, self.datafile) = tempfile.mkstemp()
        open(self.datafile, 'w').write(testdata)
    
    def tearDown(self):
        os.remove(self.datafile)
    
    def test_simple_read(self):
        f = seek_open(self.datafile)
        self.failUnlessEqual(f.next(), '12345678\n')
        self.failUnlessEqual(f.next(), 'ab\n')
        self.failUnlessEqual(f.next(), '3456789\n')
        self.failUnlessEqual(f.next(), 'cdef\n')
        self.failUnlessEqual(f.next(), '87654\n')
        self.failUnlessEqual(f.next(), 'pqrstuvw\n')
        self.failUnlessRaises(StopIteration, lambda: f.next())
    
    def test_seek_read(self):
        f = seek_open(self.datafile, 10)
        self.failUnlessEqual(f.next(), '3456789\n')
    
    def test_end_read(self):
        f = seek_open(self.datafile, end=2)
        self.failUnlessEqual(f.next(), '12345678\n')
        self.failUnlessRaises(StopIteration, lambda: f.next())
        
        f = seek_open(self.datafile, end=8)
        self.failUnlessEqual(f.next(), '12345678\n')
        self.failUnlessRaises(StopIteration, lambda: f.next())
        
        f = seek_open(self.datafile, end=9)
        self.failUnlessEqual(f.next(), '12345678\n')
        self.failUnlessEqual(f.next(), 'ab\n')
        self.failUnlessRaises(StopIteration, lambda: f.next())
        
        f = seek_open(self.datafile, end=9)
        data = ''
        for line in f:
            data += line
        self.failUnlessEqual(data, '12345678\nab\n')
    
    def test_seek_and_end_read(self):
        f = seek_open(self.datafile, 8, 14)
        self.failUnlessEqual(f.next(), 'ab\n')
        self.failUnlessEqual(f.next(), '3456789\n')
        self.failUnlessRaises(StopIteration, lambda: f.next())
        
        f = seek_open(self.datafile, 9, 14)
        self.failUnlessEqual(f.next(), '3456789\n')
        self.failUnlessRaises(StopIteration, lambda: f.next())

def big_file_test(file_name, n_pieces):
    print "testing specified file (%s) in %d pieces" % (file_name, n_pieces)
    
    def read_seek():
        for (start, end) in file_offsets(os.path.getsize(file_name), n_pieces):
            # print "reading piece: %d -> %d" % (start, end)
            for line in seek_open(file_name, start, end):
                yield line
    
    seek_data = read_seek()
    
    for line in open(file_name, 'rb'):
        line2 = seek_data.next()
        if line != line2:
            print "ERROR"
            sys.exit()


if __name__ == '__main__':
    if len(sys.argv) > 2:
        big_file_test(sys.argv[1], int(sys.argv[2]))
    else:
        unittest.main()
