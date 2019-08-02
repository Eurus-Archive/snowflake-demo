"""
Generate distributed-id implementation Twitter id's Snowflake with Python
"""
import functools
import threading
import time
import uuid


def synchronized(wrapped):
    lock = threading.Lock()

    @functools.wraps(wrapped)
    def _wrap(*args, **kwargs):
        # print("Calling '%s' with Lock %s" % (wrapped.__name__, id(lock)))
        with lock:
            return wrapped(*args, **kwargs)

    return _wrap


class IdWorker:

    def __init__(self, worker_id, datacenter_id, sequence):
        self.worker_id = worker_id
        self.datacenter_id = datacenter_id
        self.sequence = sequence

        self.epoch = 1288834974657
        self.worker_id_bits = 5
        self.max_worker_id_bits = -1 ^ (-1 << self.worker_id_bits)
        self.datacenter_id_bits = 5
        self.max_datacenter_id_bits = -1 ^ (-1 << self.datacenter_id_bits)
        self.sequence_bits = 12
        self.sequence_mask = -1 ^ (-1 << self.sequence_bits)
        self.worker_id_shift = self.sequence_bits
        self.datacenter_id_shift = self.worker_id_bits + self.sequence_bits
        self.timestamp_left_shift = self.datacenter_id_bits + self.worker_id_bits + self.sequence_bits
        self.last_timestamp = -1

    def get_id(self):
        return self.next_id()

    @synchronized
    def next_id(self):
        timestamp = self.time_gen()
        if timestamp < self.last_timestamp:
            return 'error'
        if self.last_timestamp == timestamp:
            self.sequence = (self.sequence + 1) & self.sequence_mask
            if self.sequence == 0:
                timestamp = self.till_next_milliseconds(self.last_timestamp)
        else:
            self.sequence = 0
        self.last_timestamp = timestamp
        next_id = ((timestamp - self.epoch) << self.timestamp_left_shift) | (
                self.datacenter_id << self.datacenter_id_shift) | (
                          self.worker_id << self.worker_id_shift) | self.sequence
        return next_id

    def till_next_milliseconds(self, last_timestamp):
        timestamp = self.time_gen()
        while timestamp <= last_timestamp:
            timestamp = self.time_gen()
        return timestamp

    @staticmethod
    def time_gen():
        timestamp = int(round(time.time() * 1000))
        return timestamp


if __name__ == '__main__':
    worker = IdWorker(1, 1, 0)
    print("Snowflake generate numbers of 1000000 ids ")
    s = time.time()
    for i in range(1000000):
        worker.get_id()
    print("time spend {}".format(time.time() - s))

    print("UUID generate numbers of 1000000 ids")
    s = time.time()
    for i in range(1000000):
        uuid.uuid4()
    print("time spend {}".format(time.time() - s))
