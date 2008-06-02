import time
import os
from optparse import OptionParser

def seek_open(file_name, start=0, end=-1):
    f = open(file_name, 'rb')
    
    length = start
    
    if start > 0:
        f.seek(start)
        length += len(f.next())
    
    for line in f:
        yield line
        
        length += len(line)
        if end > -1 and length > end:
            return

def file_offsets(file_size, chunks):
    offsets = list([int(file_size/chunks * n) for n in range(chunks)])
    offsets.append(file_size)
    return map(lambda s, e: (s, e), offsets[:-1], offsets[1:])

def parse_argv():
    parser = OptionParser()
    parser.add_option("-q", "--quiet", dest="quiet", action='store_true', default=False)
    parser.add_option("-c", "--cores", dest="cores", type='int', default=1)
    parser.add_option("-j", "--jobs-per-core", type='float', dest='jobs_per_core', default=1.)
    parser.add_option("-l", "--log-file", dest='log_file', default=None)
    parser.add_option("--log-each-job", dest='log_each_job', action='store_true', default=False)
    (options, args) = parser.parse_args()
    
    options.filename = args[0]
    
    return options

class Stats:
    def __init__(self, log_file, log_each_job=False):
        self.wait_time = 0
        self.join_result_time = 0
        self.total_jobs_time = 0
        self.prev_time = None
        self.log_file = log_file
        self.report_time = 0
        self.log_each_job = log_each_job
    
    def initial_report(self, file_name, file_size, chunks):
        self.n_jobs = chunks
        self.begin_time = time.clock()
        self._output("[master process] (%d) starting %s (%.1f MB: %d chunks of %.1f MB each)" %
                (os.getpid(), file_name, file_size / 2**20, chunks, float(file_size) / chunks / 2**20))
    
    def waiting(self):
        now_time = time.clock()
        if self.prev_time:
            self.wait_time += now_time - self.prev_time
        self.prev_time = now_time
    
    def received_job_result(self):
        now_time = time.clock()
        self.join_result_time += now_time - self.prev_time
        self.prev_time = now_time
    
    def job_report(self, job_pid, job_time):
        if self.log_each_job:
            self._output("[job %d] %.3f" % (job_pid, job_time))
        self.total_jobs_time += job_time
    
    def report_master_stats(self):
        self._output("[master process] (%d) done. total time: %.3f; avg job time: %.3f" %
                (os.getpid(), time.clock() - self.begin_time, self.total_jobs_time / self.n_jobs))
        self._output("[master process] idle time: %.3f; join time: %.3f; report time: %.3f" %
                (self.wait_time, self.join_result_time, self.report_time))
    
    def begin_report(self):
        self.report_start = time.clock()
    
    def done_report(self):
        self.report_time = time.clock() - self.report_start
    
    def _output(self, message):
        if not self.log_file:
            return
        
        if self.log_file == '-':
            print message
        else:
            f = open(self.log_file, 'a')
            f.write(message + "\n")
            f.close()
