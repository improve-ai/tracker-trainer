

def select_incoming_history_files(stats):
    # hash based on the first 8 characters of the hashed history id
    return select_files_for_node(INCOMING_HISTORIES_PATH, '*.jsonl.gz', stats)
    # TODO check valid file name & hashed history id chars
    
def save_history(hashed_history_id, history_records):
    
    output_file = history_dir_for_hashed_history_id(hashed_history_id) / f'{hashed_history_id}-{uuid.uuid4()}.jsonl.gz'
    save_gzipped_jsonlines(output_file, history_records)

def unique_hashed_history_file_name(hashed_history_id):
    return f'{hashed_history_id}-{uuid.uuid4()}.jsonl.gz'
    
def hashed_history_id_from_file(file):
    return file.name.split('-')[0]

def history_dir_for_hashed_history_id(hashed_history_id):
    # returns a path like /mnt/histories/1c/aa
    return config.HISTORIES_PATH / sub_dir_for_hashed_history_id(hashed_history_id)

def incoming_history_dir_for_hashed_history_id(hashed_history_id):
    # returns a path like /mnt/incoming_histories/1c/aa
    return config.INCOMING_HISTORIES_PATH / sub_dir_for_hashed_history_id(hashed_history_id)

def sub_dir_for_hashed_history_id(hashed_history_id):
    # returns a path like /mnt/histories/1c/aa
    return hashed_history_id[0:2] / hashed_history_id[2:4]

def history_files_for_hashed_history_id(hashed_history_id, stats):
    results = list(history_dir_for_hashed_history_id(hashed_history_id).glob(f'{hashed_history_id}-*.jsonl.gz'))
    stats[PROCESSED_HISTORY_FILE_COUNT] += len(results)
    return results

def group_files_by_hashed_history_id(files):
    sorted_files = sorted(files, key=hashed_history_id_from_file)
    return [list(it) for k, it in groupby(sorted_files, hashed_history_id_from_file)]    
