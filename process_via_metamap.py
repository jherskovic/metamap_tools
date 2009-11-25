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

METAMAP_BINARY="/opt/public_mm/bin/metamap09 -Z 08 -iDN --no_header_info"

def log_error_line(troublesome_line):
    open("error_lines.log", "a").write("%s\n" % troublesome_line.strip())

class LineProcessor(Process):
    def run(self):
        self.orig_id.value, the_freaking_line=self.the_freaking_line.value.split('|', 1)
        try:
            self.mm_exe=subprocess.Popen(METAMAP_BINARY, 
                                stdin=subprocess.PIPE, 
                                stdout=subprocess.PIPE,
                                universal_newlines=True,
                                shell=True,
                                #bufsize=-1,
                                ) # 10 mb per process of buffer
            #self.mm_exe.stdin.write('%s\n' % self.the_freaking_line.value)
            #mm_exe.stdin.flush()
            #mm_exe.stdin.close()
            #results=mm_exe.stdout.read(100)
            #results=mm_exe.stdout.read()
            time.sleep(0.05)
            results=self.mm_exe.communicate('%s\n' % self.the_freaking_line.value)[0]
        except:
            self.mm_exe.kill()
            log_error_line(self.the_freaking_line.value)
            self.returnvalue.value="%s|*** error ***" % self.orig_id.value
            return
        # Discard everything that's not a result line
        self.returnvalue.value='\n'.join([x.replace("00000000", self.orig_id.value)
                          for x in results.split('\n') if x[:8]=="00000000"])
        return
        
def process_one_line(the_freaking_line):
    monitored_process=LineProcessor()
    monitored_process.the_freaking_line=Array('c', the_freaking_line)
    monitored_process.returnvalue=Array('c', 256*1024)
    monitored_process.orig_id=Array('c', 256)
    monitored_process.start()
    monitored_process.join(20) # Wait for, at the most, 20 seconds
    if monitored_process.is_alive():
        print "Warning: terminating runaway metamap process"
        monitored_process.terminate()
        monitored_process.mm_exe.kill()
        log_error_line(the_freaking_line)
        return "%s|*** error ***" % monitored_process.orig_id
    return monitored_process.returnvalue.value
    
def process_queue(which_queue, output_queue):
    while True:
        request=which_queue.get()
        # print "Got request", request
        if request=='STOP':
            which_queue.task_done()
            break
        try:
            get_response=process_one_line(request)
            if get_response is not None:
                output_queue.put(get_response)
        finally:
            which_queue.task_done()
    return
    
def retrieve_output(output_queue, output_file):
    start_time=time.time()
    items=0
    while True:
        output_item=output_queue.get()
        # print "Got ", output_item
        if output_item is None:
            #output_queue.task_done()
            break
        elapsed=time.time()-start_time
        items+=1
        try:
            speed=items/elapsed
        except ZeroDivisionError:
            speed=items
        output_file.write('%s\n' % output_item)
        print "%d items processed in %1.2f sec (%1.2f items/sec)" % (
                                                        items, elapsed, speed)
    return
    
def main():
    workers=cpu_count()
    line_queue=JoinableQueue(workers*2) # Keep at most 2*workers lines in flight
    input_file=open(sys.argv[1], 'rU')
    output_file=open(sys.argv[2], 'w')
    output_queue=Queue(workers*3000)
    processes=[]
    for i in xrange(workers):
        this_process=Process(target=process_queue,
                             args=(line_queue, output_queue))
        this_process.start()
        processes.append(this_process)

    # Start the output processor
    output_processor=Process(target=retrieve_output, 
                             args=(output_queue, output_file))
    output_processor.start()

    for l in input_file:
        line_queue.put(l)

    for i in xrange(workers):
        line_queue.put('STOP')
    
    print "Waiting for all tasks to end."
    line_queue.close()
    line_queue.join()
    print "All tasks ended. Dumping the final output."
    output_queue.put(None)
    output_queue.close()
    output_processor.join()
    
    print "Done. Exiting."
    output_file.close()
    
    return
    
if __name__ == '__main__':
    main()

