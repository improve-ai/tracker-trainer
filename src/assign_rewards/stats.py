import threading
import time

class Stats():
    def __init__(self):
        self._lock = threading.Lock()
        self.bad_firehose_record_count = 0
        self.uncrecoverable_file_count = 0
        self.incoming_history_files_written_count = 0
        self.duplicate_message_id_count = 0

    def incrementBadFirehoseRecordCount(self, increment=1):
        with self._lock:
            self.bad_firehose_record_count += increment

    def incrementUnrecoverableFileCount(self, increment=1):
        with self._lock:
            self.uncrecoverable_file_count += increment

    def incrementIncomingHistoryFilesWrittenCount(self, increment=1):
        with self._lock:
            self.incoming_history_files_written_count += increment

    def incrementDuplicateMessageIdCount(self, increment=1):
        with self._lock:
            self.duplicate_message_id_count += increment
