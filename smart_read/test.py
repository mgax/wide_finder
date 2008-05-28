from unittest import TestCase, main
from read import seek_open

class SmartReadTestCase(TestCase):
    def test_simple_read(self):
        f = seek_open('data.txt')
        self.failUnlessEqual(f.next(), '12345678\n')
        self.failUnlessEqual(f.next(), 'ab\n')
        self.failUnlessEqual(f.next(), '3456789\n')
        self.failUnlessEqual(f.next(), 'cdef\n')
        self.failUnlessEqual(f.next(), '87654\n')
        self.failUnlessEqual(f.next(), 'pqrstuvw\n')
        self.failUnlessRaises(StopIteration, lambda: f.next())
    
    def test_seek_read(self):
        f = seek_open('data.txt', 9)
        self.failUnlessEqual(f.next(), 'ab\n')
    
    def test_end_read(self):
        f = seek_open('data.txt', end=2)
        self.failUnlessEqual(f.next(), '12345678\n')
        self.failUnlessRaises(StopIteration, lambda: f.next())
        
        f = seek_open('data.txt', end=8)
        self.failUnlessEqual(f.next(), '12345678\n')
        self.failUnlessRaises(StopIteration, lambda: f.next())
        
        f = seek_open('data.txt', end=9)
        self.failUnlessEqual(f.next(), '12345678\n')
        self.failUnlessEqual(f.next(), 'ab\n')
        self.failUnlessRaises(StopIteration, lambda: f.next())
        
        f = seek_open('data.txt', end=9)
        data = ''
        for line in f:
            data += line
        self.failUnlessEqual(data, '12345678\nab\n')
    
    def test_seek_and_end_read(self):
        f = seek_open('data.txt', 9, 14)
        self.failUnlessEqual(f.next(), 'ab\n')
        self.failUnlessEqual(f.next(), '3456789\n')
        self.failUnlessRaises(StopIteration, lambda: f.next())

if __name__ == '__main__':
    main()
