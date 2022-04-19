import os
import threading
from collections import defaultdict
from functools import reduce
import json
import time


class Store:
    """
    To keep counts (P)RDRs per model and per partition coming from an 
    S3's JSONL file and from an S3's Parquet file.
    """

    def __init__(self):
        """
        store = {
            'model_A': [
                {'from_parquet_s3': 15, 'from_partition': 100}, # Corresponding to one partition
                {'from_parquet_s3': 5, 'from_partition': 50},   # Corresponding to another partition
                { ... }
            ],
            'model_B': [ ... ].
            ...
        }
        """
        self.store = defaultdict(list)


    def add_count_to_model(self, model_name, count_from_partition, count_from_parquet_s3):
        """
        Store, for a model name, a pair of numbers: 
          - count of elements coming originally from a JSONL file
          - count of elements coming from an S3 Parquet file
        """

        self.store[model_name].append({
            'from_partition': count_from_partition,
            'from_parquet_s3': count_from_parquet_s3
        })


    def summarize_all(self):
        """
        Sum up the total number of elements available across all model
        names and partitions.
        """
        
        total_from_partitions = 0; total_from_s3 = 0
        for model, partitions in self.store.items():
        
            total_from_partitions += reduce(self.__sum_from_partitions, partitions, 0)
            total_from_s3 += reduce(self.__sum_from_s3, partitions, 0)

        return total_from_partitions, total_from_s3


    def total_jsonl_records_per_model(self):
        summary = {}
        for model, partitions in self.store.items():
            summary[model] = reduce(self.__sum_from_partitions, partitions, 0)
        return json.dumps(summary, indent=4) 
        
    
    def __repr__(self):
        return json.dumps(self.store, indent=4)


    def __sum_from_s3(self, acc, p):
        """For a reduce operation"""
        return  acc + p['from_parquet_s3']
    

    def __sum_from_partitions(self, acc, p):
        """For a reduce operation"""
        return  acc + p['from_partition']


class Timer:
    def __init__(self):
        self.t_store = defaultdict(dict)

    def start(self, section_name):
        if self.t_store.get(section_name, {}).get('start'):
            raise RuntimeError(f"You were about to overwrite a time!")
        self.t_store[section_name]['start'] = time.time()
        print(f"Starting timer for '{section_name}'")
    
    def stop(self, section_name):
        if self.t_store.get(section_name, {}).get('stop'):
            raise RuntimeError(f"You were about to overwrite a time!")
        self.t_store[section_name]['stop'] = time.time()
        self.took(section_name)

    def took(self, section_name):
        seconds = self.t_store[section_name]['stop'] - self.t_store[section_name]['start']
        self.t_store[section_name]['took'] = seconds
        if seconds >= 0.1:
            print(f"Stopping timer for '{section_name}', took: {seconds:.1f} s")
        else:
            print(f"Stopping timer for '{section_name}', took: {1000*seconds:.1f} ms")

    def __repr__(self):
        return json.dumps(self.t_store, indent=4)

    def __str__(self):
        return json.dumps(self.t_store, indent=4)


class Stats:
    def __init__(self):
        self._lock = threading.Lock()
        self.parse_exception_counts = {}
        self.valid_records_count = 0
        self.invalid_records_count = 0
        self.bad_s3_parquet_names = []
        self.store = Store()
        self.records_after_merge_count = 0
        self.s3_requests_count = defaultdict(int)
        # e.g. [3, 4, 5] means a set of 3 overlapping keys, a set of 4 overlapping keys, etc
        self.counts_of_set_of_overlapping_s3_keys = []
        self.timer = Timer()


    def add_parse_exception(self, e):
        """Exceptions found when loading records from a JSONL file."""
        e_str = str(e)
        with self._lock:
            self.parse_exception_counts[e_str] = self.parse_exception_counts.get(e_str, 0) + 1


    def increment_valid_records_count(self, increment=1):
        """ To count validated records loaded from a JSONL file"""
        with self._lock:
            self.valid_records_count += increment


    def increment_invalid_records_count(self, increment=1):
        """ To count invalid records loaded from a JSONL file"""
        with self._lock:
            self.invalid_records_count += increment


    def remember_bad_s3_parquet_file(self, filename, increment=1):
        """ To count the number of bad .parquet files found in S3 """
        with self._lock:
            self.bad_s3_parquet_names.append(filename)


    def increment_rewarded_decision_count(self, model_name, rdrs_from_partition, rdrs_from_s3=0):
        """ To count the number of records per model and per partition 
        coming from the JSONL file and from the S3's Parquet files. """

        with self._lock:
            self.store.add_count_to_model(model_name, rdrs_from_partition, rdrs_from_s3)


    def increment_records_after_merge_count(self, increment=1):
        """ Count the number of (P)RDRs after being merged with their S3 counterparts """
        with self._lock:
            self.records_after_merge_count += increment


    def increment_s3_requests_count(self, req_type, increment=1):
        """
        To count the number of S3 requests made (to retrieve, save or delete records)
        
        Parameters
        ----------
        req_type: str
            Request type made to S3
        increment: int
            Quantity to increment
        """
        with self._lock:
            assert req_type in ['list', 'put', 'get', 'delete']
            self.s3_requests_count[req_type] += increment


    def increment_counts_of_set_of_overlapping_s3_keys(self, increment=1):
        """
        To keep track of the count of overlapping sets and number of 
        overlapping keys per set
        """
        
        with self._lock:
            self.counts_of_set_of_overlapping_s3_keys.append(increment)


    def reset(self):
        """ Reset all attributes to zero. Useful in tests, to start clean at some point. """

        print(f"Resetting stats...")
        
        self.parse_exception_counts = {}
        self.valid_records_count = 0
        self.invalid_records_count = 0
        self.bad_s3_parquet_names = []
        self.records_after_merge_count = 0
        self.s3_requests_count = defaultdict(int)       
        self.counts_of_set_of_overlapping_s3_keys = []
        self.store = Store()
        self.timer = Timer()


    def __str__(self):
        with self._lock:
            return (
                f'Valid JSONL records: {self.store.total_jsonl_records_per_model()}\n'
                f'Invalid JSONL records: {self.invalid_records_count} \n'
                f'JSONL parsing exceptions: {self.parse_exception_counts}\n'
                f'Bad Parquet files: {self.bad_s3_parquet_names}\n'
                f'S3 requests: {sum(self.s3_requests_count.values())}\n{json.dumps(self.s3_requests_count, indent=4)} \n'
                f'Records after merge: {self.records_after_merge_count} \n'
                f'{sum(self.counts_of_set_of_overlapping_s3_keys)} overlapping records turned into {len(self.counts_of_set_of_overlapping_s3_keys)}\n'
            )
