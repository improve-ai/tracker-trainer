import os
import json
import re
import gzip
import hashlib
from uuid import uuid4
from itertools import groupby
from datetime import datetime
from operator import itemgetter
import assign_rewards

import config
import utils
import customize
from history_record import _all_valid_records
from history_record import _is_valid_model_name
from history_record import _load_records

HASHED_HISTORY_ID_REGEXP = "^[a-f0-9]+$"

HASHED_HISTORY_ID_LEN = 64

class History:
    
    def __init__(self, hashed_history_id, files):
        # get the hashed history id
        self.hashed_history_id = hashed_history_id
        self.files = files

        
    def process(self):

        # load all records, ignoring subsequent records with duplicate message_ids
        self.load()
    
        # write the consolidated records to a new history file
        # save prior to validation so that invalid records are retained
        # save prior to customization and reward assignment since those may mutate records
        self.save()
    
        # perform deeper validation before reward assignement
        self.filter_valid_records()

        self.sort_records()

        # assign rewards to decision records.
        self.assign_rewards()

        self.upload_rewarded_decisions()
    
        # delete processed incoming and history files.
        # do this last in case there is a problem during processing that needs to be retried
        self.clean_up()
    
    
    def load(self):
        self.records = []
        self.mutated = False

        message_ids = set()
    
        # iterate the files. later records with duplicate message ids will be ignored
        for file in self.files:
            self.records.extend(_load_records(file, message_ids))
    

    def save(self):
        assert not self.mutated # double check that we're not persisting modified or filtered records
        
        output_file = history_dir_for_hashed_history_id(self.hashed_history_id) / f'{self.hashed_history_id}-{uuid4()}.jsonl.gz'
    
        utils.ensure_parent_dir(output_file)
    
        # save all records
        utils.save_gzipped_jsonlines(output_file.absolute(), map(lambda x: x.loaded_json_dict, self.records))
        
        
    def clean_up(self):
        # delete the incoming and history files that were processed
        utils.delete_all(self.files)
        
        
    def filter_valid_records(self):
        self.mutated = True
        
        self.records = list(filter(lambda x: x.is_valid(), self.records))
        
        config.stats.incrementValidatedRecordCount(len(self.records))


    def sort_records(self):
        # sort by timestamp. On tie, records of type 'decision' are sorted earlier
        self.records.sort(key=lambda x: (x.timestamp, 0 if x.is_decision_record() else 1))

        
    def assign_rewards(self):
        self.mutated = True

        assign_rewards.assign_rewards(self.records)

    
    def upload_rewarded_decisions(self):
        # extract decision records
        decision_records = list(filter(lambda x: x.is_decision_record(), self.records))
        
        # sort by model for groupby
        decision_records.sort(key = lambda x: x.model)
        for model_name, decision_record_group in groupby(decision_records, lambda x: x.model):
    
            # filter out any keys that don't need to be used in training
            rewarded_decision_dicts = list(map(lambda x: x.to_rewarded_decision_dict(), decision_record_group))

            s3_key = rewarded_decisions_s3_key(model_name, self.hashed_history_id)
            
            utils.upload_gzipped_jsonlines(config.TRAIN_BUCKET, s3_key, rewarded_decision_dicts)
            
            config.stats.addModel(model_name)
            config.stats.incrementRewardedDecisionCount(len(rewarded_decision_dicts))
                
    
def histories_to_process():
    histories = []
    
    # identify the portion of incoming history files to process in this node
    incoming_history_files = select_incoming_history_files_for_node()
    for hashed_history_id, incoming_history_file_group in \
            groupby(sorted(incoming_history_files, key=hashed_history_id_from_file), hashed_history_id_from_file):
    
        files = list(incoming_history_file_group)
        
        # sort in reverse chronological order so the newest incoming files are at the beginning 
        # of the list so that they get precedence for duplicate message ids
        files.sort(key=os.path.getctime, reverse=True)

        # list the previously saved history files for this hashed_history_id
        history_files = history_files_for_hashed_history_id(hashed_history_id)

        # seperately sort the previously saved history files in reverse chronological order so
        # that the most recent saved history files get precedence for duplicate message ids
        history_files.sort(key=os.path.getctime, reverse=True)
        
        # add any previously saved history files for this hashed history id to
        # the end of the file group. In the event of duplicate message_ids,
        # the incoming history files will take precedence because they are
        # earlier in the file group.
        files.extend(history_files)
    
        histories.append(History(hashed_history_id, files))

    return histories


def select_incoming_history_files_for_node():
    files = []
    file_count = 0
    glob = '*.jsonl.gz'
    for f in config.INCOMING_PATH.glob(glob):
        if not is_valid_hashed_history_id(hashed_history_id_from_file(f)):
            print(f'skipping bad file name {f}')
            config.stats.incrementBadFileNameCount()
            continue

        file_count += 1
        # convert first 8 hex chars (32 bit) to an int
        # the file name starts with the hashed history id
        # check if int mod node_count matches our node
        if (int(f.name[:8], 16) % config.NODE_COUNT) == config.NODE_ID:
            files.append(f)

    print(f'selected {len(files)} of {file_count} files from {config.INCOMING_PATH}/{glob} to process')

    return files


def unique_hashed_history_file_name(hashed_history_id):
    return f'{hashed_history_id}-{uuid4()}.jsonl.gz'


def hashed_history_id_from_file(file):
    return file.name.split('-')[0]


def history_dir_for_hashed_history_id(hashed_history_id):
    # returns a path like /mnt/histories/1c/aa
    return config.HISTORIES_PATH / hashed_history_id[0:2] / hashed_history_id[2:4]


def history_files_for_hashed_history_id(hashed_history_id):
    results = \
        list(
            history_dir_for_hashed_history_id(hashed_history_id)
                .glob(f'{hashed_history_id}-*.jsonl.gz'))
    return results

def is_valid_hashed_history_id(hashed_history_id):
    # illegal chars or bad length of sha256 chunk
    if not isinstance(hashed_history_id, str) \
            or len(hashed_history_id) != HASHED_HISTORY_ID_LEN \
            or not re.match(HASHED_HISTORY_ID_REGEXP, hashed_history_id):
        return False
        
    return True
        
def hash_history_id(history_id):
    return hashlib.sha256(history_id.encode()).hexdigest()

def rewarded_decisions_s3_key(model, hashed_history_id):
    assert _is_valid_model_name(model)
    assert is_valid_hashed_history_id(hashed_history_id)

    return f'rewarded_decisions/{model}/{hashed_history_id[0:2]}/{hashed_history_id[2:4]}/{hashed_history_id}.jsonl.gz'

