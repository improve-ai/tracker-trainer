import threading
import time

class Stats():
    def __init__(self):
        self._lock = threading.Lock()
        self.history_record_count = 0
        self.rewarded_decision_count = 0
        self.uncrecoverable_file_count = 0
        self.duplicate_message_id_count = 0
        self.models = set()

    def incrementHistoryRecordCount(self, increment=1):
        with self._lock:
            self.history_record_count += increment

    def incrementUnrecoverableFileCount(self, increment=1):
        with self._lock:
            self.uncrecoverable_file_count += increment

    def incrementRewardedDecisionCount(self, increment=1):
        with self._lock:
            self.rewarded_decision_count += increment

    def incrementDuplicateMessageIdCount(self, increment=1):
        with self._lock:
            self.duplicate_message_id_count += increment
            
    def addModel(self, modelName):
        with self._lock:
            self.models.add(modelName)

    def __str__(self):
        with self._lock:
            return (f'Loaded History Records: {self.history_record_count}\n'+
            f'Uploaded Rewarded Decisions: {self.rewarded_decision_count}\n'
            f'Unrecoverable Files: {self.uncrecoverable_file_count}\n'+
            f'Duplicate Message Ids: {self.duplicate_message_id_count}\n'+
            f'Models: {self.models}\n')
