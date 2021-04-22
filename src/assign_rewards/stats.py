import threading
import time

class Stats():
    def __init__(self):
        # RawValue because we don't need it to create a Lock:
        self.val = RawValue('i', value)
        self._lock = threading.Lock()

    def increment(self):
        with self._lock:
            self.val.value += 1
