import random
import ctypes

import dask.bag as db
import dask.dataframe as dd


def random_model_seed():
    return random.getrandbits(32)


def str2bool(v):
  return v.lower() in ("yes", "true", "t", "1")


# https://stackoverflow.com/questions/47812785/remove-empty-partitions-in-dask
def cull_empty_partitions(bag):
    """
    When bags are created by filtering or grouping from a different bag,
    it retains the original bag's partition count, even if a lot of the
    partitions become empty.
    Those extra partitions add overhead, so it's nice to discard them.
    This function drops the empty partitions.
    """
    bag = bag.persist()
    def get_len(partition):
        # If the bag is the result of bag.filter(),
        # then each partition is actually a 'filter' object,
        # which has no __len__.
        # In that case, we must convert it to a list first.
        if hasattr(partition, '__len__'):
            return len(partition)
        return len(list(partition))
    partition_lengths = bag.map_partitions(get_len).compute()

    # Convert bag partitions into a list of 'delayed' objects
    lengths_and_partitions = zip(partition_lengths, bag.to_delayed())

    # Drop the ones with empty partitions
    partitions = (p for l,p in lengths_and_partitions if l > 0)

    # Convert from list of delayed objects back into a Bag.
    return db.from_delayed(partitions)


def cull_empty_df_partitions(df):
    ll = list(df.map_partitions(len).compute())
    df_delayed = df.to_delayed()
    df_delayed_new = list()
    pempty = None
    for ix, n in enumerate(ll):
        if 0 == n:
            pempty = df.get_partition(ix)
        else:
            df_delayed_new.append(df_delayed[ix])
    if pempty is not None:
        df = dd.from_delayed(df_delayed_new, meta=pempty)
    return df


def trim_memory() -> int:
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)
