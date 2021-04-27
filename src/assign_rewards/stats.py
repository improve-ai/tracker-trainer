import threading

class Stats():
    def __init__(self):
        self._lock = threading.Lock()
        self.history_record_count = 0
        self.rewarded_decision_count = 0
        self.duplicate_message_id_count = 0
        self.uncrecoverable_file_count = 0
        self.models = set()

    def incrementHistoryRecordCount(self, increment=1):
        with self._lock:
            self.history_record_count += increment

    def incrementRewardedDecisionCount(self, increment=1):
        with self._lock:
            self.rewarded_decision_count += increment

    def incrementDuplicateMessageIdCount(self, increment=1):
        with self._lock:
            self.duplicate_message_id_count += increment

    def incrementUnrecoverableFileCount(self, increment=1):
        with self._lock:
            self.uncrecoverable_file_count += increment
            
    def addModel(self, modelName):
        with self._lock:
            self.models.add(modelName)

    def __str__(self):
        with self._lock:
            return (f'history records loaded: {self.history_record_count}\n'
            f'rewarded decisions uploaded: {self.rewarded_decision_count}\n'
            f'duplicate message ids: {self.duplicate_message_id_count}\n'
            f'unrecoverable files: {self.uncrecoverable_file_count}\n'
            f'models: {self.models}\n')
