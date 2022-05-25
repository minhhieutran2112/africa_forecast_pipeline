# -*- coding: utf-8 -*-
"""
This module houses the base for every multiprocessor used in the scripts
"""

import multiprocessing
from queue import Empty

class multiprocessor():
    """
    Class to do multiprocessing
    """
    
    def __init__(self,task_ids,num_cpu):
        self.manager = multiprocessing.Manager()
        self.pool = multiprocessing.Pool(num_cpu)
        self.queue = self.manager.Queue()
        self.completed = self.manager.dict()
        self.num_cpu=num_cpu

        for i in task_ids:
            self.queue.put(i)

    def process(self,index):
        pass

    def worker(self):
        try:
            while True:
                index=self.queue.get(False)
                if index in self.completed:
                    continue
                done=False
                while not done:
                    try:
                        self.process(index)
                        done=True
                    except Exception as e:
                        pass
                self.queue.task_done()
        except Empty:
            pass

    def do_task(self):
        results = [self.pool.apply_async(self.worker,(self.process)) for i in range(self.num_cpu)]
        return [l.get() for l in results]