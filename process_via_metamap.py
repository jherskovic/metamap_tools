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
from multiprocessing import (Queue, JoinableQueue, cpu_count, Process, 
                             current_process, get_logger, Array)
import subprocess
import time
import re
import traceback

METAMAP_BINARY="/opt/public_mm/bin/metamap09 -Z 08 -iDN --no_header_info"

# These are the lines that interest us in MetaMap's output
metamap_output_filter=re.compile(r'^\d+\|.*', re.MULTILINE)

def log_error_line(troublesome_line):
    open("error_lines.log", "a").write("%s\n" % troublesome_line.strip())

class LineProcessor(Process):
    def run(self):
        try:
            mm_exe=subprocess.Popen(METAMAP_BINARY, 
                                stdin=subprocess.PIPE, 
                                stdout=subprocess.PIPE,
                                universal_newlines=True,
                                shell=True,
                                #bufsize=-1,
                                ) # 10 mb per process of buffer
            # While we are opening the process, let's prepare the input
            template="UI  - %s\nTX  - %s\n\n"
            input_tuples=[tuple(x.strip().split('|', 1)) for x in self.data]
            the_input=''.join(template % x for x in input_tuples)
            time.sleep(0.05)
            results=mm_exe.communicate('%s' % the_input)[0]
        except:
            print "Exception:", traceback.format_exc(), "on", self.data
            try:
                mm_exe.kill()
            except:
                log_error_line('Unable to kill process %r' % mm_exe)
            log_error_line(''.join(self.data))
            troublesome_ids=[x.split('|', 1)[0] for x in self.data]
            self.returnvalue.value='\n'.join("%s|*** error ***" % x 
                                            for x in troublesome_ids)
            return
        # Discard everything that's not a result line
        self.returnvalue.value='\n'.join(metamap_output_filter.findall(results))
        return
        
def process_several_lines(lines):
    monitored_process=LineProcessor()
    monitored_process.data=lines
    monitored_process.returnvalue=Array('c', 256*1024*len(lines)) # 256 kb per line 
                                                                  # should be enough
                                                                  # for anybody
    monitored_process.start()
    monitored_process.join(10*len(lines)) # Wait for, at the most, 10 seconds per line
    if monitored_process.is_alive():
        print "Warning: terminating runaway metamap process"
        monitored_process.kill()
        log_error_lines(''.join(lines))
        troublesome_ids=[x.split('|', 1)[0] for x in lines]
        return '\n'.join("%s|*** error ***" % x for x in troublesome_ids)
    return monitored_process.returnvalue.value
    
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
        finally:
            which_queue.task_done()
    return
    
def retrieve_output(output_queue, output_file, number_of_lines_at_once=10):
    start_time=time.time()
    items=0
    next_block=0
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
            output_file.write('%s\n' % waiting_blocks.pop(0)[1])
            next_block+=1
        print "%d items processed and %d output in %1.2f sec (%1.2f items/sec)" % (
                                                    items, 
                                                    next_block*number_of_lines_at_once,
                                                    elapsed, speed)
    if len(waiting_blocks)>0:
        for b in waiting_blocks:
            output_file.write("%s\n" % b[1])
    return
    
def main():
    workers=cpu_count()
    line_queue=JoinableQueue(workers*2) # Keep at most 2*workers lines in flight
    input_file=open(sys.argv[1], 'rU')
    output_file=open(sys.argv[2], 'w')
    output_queue=Queue(workers*3)
    lines_at_once=500
    processes=[]
    for i in xrange(workers):
        this_process=Process(target=process_queue,
                             args=(line_queue, output_queue, lines_at_once))
        this_process.start()
        processes.append(this_process)

    # Start the output processor
    output_processor=Process(target=retrieve_output, 
                             args=(output_queue, output_file, lines_at_once))
    output_processor.start()

    small_queue=[]
    block_number=0
    for l in input_file:
        small_queue.append(l)
        if len(small_queue)>=lines_at_once:
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

