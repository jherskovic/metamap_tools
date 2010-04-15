#!/usr/bin/env python
# encoding: utf-8
"""
process_via_metamap.py

Created by Dr. H on 2009-07-23.
Copyright (c) 2009 UTHSC School of Health Information Sciences. All rights reserved.

Licensed under the GNU GPL v2 (see gpl_v2.txt)

This script imitates the NLM's scheduler on a multicore Linux machine. It runs multiple 
instances of MetaMap to extract concepts from a "single line delimited input with ID" 
file, i.e. one that looks like this

00001111|This is the first line
00001112|This is the second line

This program expects the wsdserverctl and skrmedpostcrl daemons to have already been
started. Please start them before running this script!

Usage:
python process_via_metamap.py [input file] [output file]

Modify the METAMAP_BINARY= line to point to your MetaMap installation.
"""

import sys
import os
import signal
from multiprocessing import (Queue, JoinableQueue, cpu_count, Process, 
                             current_process, get_logger, Array, Lock)
from Queue import Empty                            
#from threading import Thread
import subprocess
import time
import re
import traceback
import tempfile
import mmap

# The location of the metamap executable
METAMAP_BINARY="/opt/public_mm/bin/metamap09 -Z 08 -iDN --no_header_info"

# The number of lines to process in each instance of MetaMap
LINES_AT_ONCE=1

# The maximum number of words per line MetaMap will process without crashing
# Arbitrarily chose 125. Who writes 100+ word sentences anyway?
MAX_WORDS_PER_LINE=200

# The maximum number of seconds to spend per line
MAX_SECONDS_PER_LINE=10

# These are the lines that interest us in MetaMap's output
metamap_output_filter=re.compile(r'^\d+\|.*', re.MULTILINE)

log_lock=Lock()

def log_error_line(troublesome_line):
    """Spits a bad line out to a file called error_lines.log in the current
    directory."""
    global log_lock
    log_lock.acquire()
    try:
        open("error_lines.log", "a").write("%s\n" % troublesome_line.strip())
    finally:
        log_lock.release()
        
def kill_process_group(bad_group):
    os.killpg(bad_group, signal.SIGKILL)

class LineProcessor(Process):
    """The LineProcessor class is a Process whose only purpose in life is to
    control and isolate a MetaMap instance. It receives input and provides
    output through Queues."""
    def __init__(self, input_queue, output_queue):
        self.input=input_queue
        self.output=output_queue
        Process.__init__(self)
    def run(self):
        try:
            os.setpgid(0, os.getpid()) # Start a new process group
            # Initialize the MetaMap process
            self.mm_exe=subprocess.Popen(METAMAP_BINARY, 
                                stdin=subprocess.PIPE, 
                                stdout=subprocess.PIPE,
                                universal_newlines=True,
                                shell=True,
                                #bufsize=-1,
                                ) # 10 mb per process of buffer
            # While we are opening the process, let's prepare the input
            template="UI  - %s\nTX  - %s\n\n"
            input_tuples=[]
            while True:
                try:
                    x=self.input.get(False)
                except Empty:
                    break
                input_tuples.append(tuple(x.strip().split('|', 1)))
            # Set aside the tuples with too many words per line
            bad_tuples=[x for x in input_tuples 
                        if len(x[1].split()) > MAX_WORDS_PER_LINE]
            bad_tuple_ids=set([x[0] for x in bad_tuples])
            the_input=''.join(template % x for x in input_tuples
                              if x[0] not in bad_tuple_ids)
            time.sleep(0.05)
            # Keep the process group handy just in case it dies on us
            self.pgrp=os.getpgid(self.mm_exe.pid)
            # Send and receive data
            results=self.mm_exe.communicate('%s' % the_input)[0]
            # Right now we just omit the bad tuples from the result set 
            # silently. Perhaps not the best solution, but it will work.
            # We will log them to the "bad line" list.
            for each_bad_tuple in bad_tuples:
                log_error_line("Line %s has too many words: %s" % 
                                each_bad_tuple)
        except:
            print "Exception:", traceback.format_exc(), "on", self.data
            try:
                self.kill_process_group(pgrp)
            except:
                log_error_line('Unable to kill process group %r' % pgrp)
            log_error_line(''.join(self.data))
            troublesome_ids=[x.split('|', 1)[0] for x in self.data]
            for x in troublesome_ids:
                self.output.put("%s|*** error ***" % x)
            return
        # Discard everything that's not a result line
        for x in metamap_output_filter.findall(results):
            self.output.put(x)
        return
        
def process_several_lines(lines):
    """Process a set of lines while enforcing a time limitation and terminating
    the MetaMap process tree if it does not finish on time."""
    to_child=Queue()
    from_child=Queue()
    monitored_process=LineProcessor(to_child, from_child)
    for x in lines:
        to_child.put(x)
    monitored_process.start()
    monitored_process.join(MAX_SECONDS_PER_LINE*len(lines)) # Wait for this at the most
    if monitored_process.is_alive():
        print "Warning: terminating runaway metamap process"
        try:
            bad_group=os.getpgid(monitored_process.pid)
            kill_process_group(bad_group)
        except:
            print "Unable to kill process group %r" % pgrp
        log_error_line(''.join(lines))
        troublesome_ids=[x.split('|', 1)[0] for x in lines]
        return '\n'.join("%s|*** error ***" % x for x in troublesome_ids)
    results=[]
    while True:
        try:
            x=from_child.get(False)
        except Empty:
            break
        results.append(x)
    return "\n".join(results)
    
def process_queue(which_queue, output_queue, number_of_lines_at_once=10):
    this_request=[]
    while True:
        request=which_queue.get()
        # print "Got request", request
        if request=='STOP':
            which_queue.task_done()
            break
        try:
            block_number=request[0]
            request_lines=request[1]
            get_response=process_several_lines(request_lines)
            if get_response is not None:
                output_queue.put((block_number, get_response))
            else:
                output_queue.put((block_number, ""))
        except:
            print "Exception:", traceback.format_exc(), "on block", block_number
            output_queue.put((block_number, ""))
        finally:
            which_queue.task_done()
    return
    
def retrieve_output(output_queue, output_file, number_of_lines_at_once=10):
    start_time=time.time()
    items=0
    next_block=0
    lines_actually_written=0
    waiting_blocks=[]
    while True:
        incoming_output=output_queue.get()
        # print "Got ", output_item
        if incoming_output is None:
            #output_queue.task_done()
            break
        elapsed=time.time()-start_time
        items+=number_of_lines_at_once
        try:
            speed=items/elapsed
        except ZeroDivisionError:
            speed=items
        waiting_blocks.append(incoming_output)
        waiting_blocks.sort()
        # The next line relies on Python's short circuit evaluation 
        while len(waiting_blocks)>0 and waiting_blocks[0][0]==next_block:
            line_block=waiting_blocks.pop(0)[1]
            output_file.write('%s\n' % line_block)
            next_block+=1
            lines_actually_written+=line_block.count('\n')
        print "%d items processed. %d received. %d lines output in %1.2f s (%1.2f items/s)" % (
                                        items, 
                                        next_block*number_of_lines_at_once,
                                        lines_actually_written,
                                        elapsed, speed)

    # This will only happen if waiting_blocks is not empty
    for b in waiting_blocks:
        output_file.write("%s\n" % b[1])
    return

def main():
    workers=cpu_count()
    line_queue=JoinableQueue(workers*2) # Keep at most 2*workers lines in flight
    input_file=open(sys.argv[1], 'rU')
    output_file=open(sys.argv[2], 'w')
    output_queue=Queue(workers*3)

    processes=[]
    for i in xrange(workers):
        this_process=Process(target=process_queue,
                             args=(line_queue, output_queue, LINES_AT_ONCE))
        this_process.start()
        processes.append(this_process)

    # Start the output processor
    output_processor=Process(target=retrieve_output, 
                             args=(output_queue, output_file, LINES_AT_ONCE))
    output_processor.start()

    small_queue=[]
    block_number=0
    for l in input_file:
        small_queue.append(l)
        if len(small_queue)>=LINES_AT_ONCE:
            line_queue.put((block_number, small_queue))
            block_number+=1
            small_queue=[]
    if len(small_queue)>0:
        line_queue.put((block_number, small_queue))
        
    for i in xrange(workers):
        line_queue.put('STOP')
    
    print "Waiting for all tasks to end."
    line_queue.close()
    line_queue.join()
    for p in processes:
        p.join()
    
    print "All tasks ended. Dumping the final output."
    output_queue.put(None)
    output_queue.close()
    output_processor.join()
    
    print "Done. Exiting."
    output_file.close()
    
    return
    
if __name__ == '__main__':
    main()

