from constants import ITEM_FEATURE_KEY, CONTEXT_FEATURE_KEY


def flatten_item(item, into=None):
    return flatten(item, ITEM_FEATURE_KEY, features=into)


def flatten_context(context, into=None):
    return flatten(context, CONTEXT_FEATURE_KEY, features=into)


def flatten(obj, path, features=None):

    if features is None:
        features = dict()

    _flatten(obj, path, features)

    return features


def _flatten(obj, path, features):
    """
    Encodes a JSON serializable object to a flat string -> string/number dict
    Rules of encoding go as follows:

    - None, json null, {}, [], are treated as missing features and ignored.

    - numbers, booleans, and strings are encoded as-is.

    - feature keys are dot notation JSON paths

    Parameters
    ----------
    obj: object
        a JSON serializable object to be encoded to a flat key-value structure
    path: string
        the current dot notation key path
    features: dict
        a flat dict of {<feature name>: <feature value>, ...} pairs

    Returns
    -------
    None
    """

    if isinstance(obj, (int, float, str)):  # bool is an instanceof int

        features[path] = obj 

    elif isinstance(obj, dict):
        for key, value in obj.items():
            _flatten(value, path + '.' + key, features)
        
    elif isinstance(obj, list): 
        for index, item in enumerate(obj):
            _flatten(item, path + '.' + str(index), features)
    
    elif obj is None:
        pass

    else: 
        raise ValueError(f'invalid object {obj}')

'''
Notes:

20221103: For JSON key paths, we're assuming a small number of key paths so that the number of features stays small. The feature count is directly proportionate to the amount
of memory used during XGBoost training and during inference on the clients. Furthermore on iOS, features are provided by a slow MLFeatureProvider interface so it's best to
keep the number of features small. The only cases where we would expect them to have large numbers of features is when using either large arrays, or attempting to model
sets by using dynamic dictionary keys. We will discourage both of these approaches. For set modeling, I recommended using a minhash with a fixed array of elements. There is
only so much complexity that XGBoost can reasonably model, so sketch based approaches like minhash are preferred.

'''