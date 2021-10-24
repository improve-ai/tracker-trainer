# External imports
from pytest_cases import fixture

# Local imports
from history_record import MESSAGE_ID_KEY, TIMESTAMP_KEY, TYPE_KEY
from history_record import MODEL_KEY
from history_record import VARIANT_KEY, GIVENS_KEY, COUNT_KEY, RUNNERS_UP_KEY, SAMPLE_KEY


@fixture

def get_record():
    """ A factory fixture.
    Advantages: https://stackoverflow.com/a/51663667/1253729
    """
    
    def __get_record(
        msg_val   = "A",
        ts_val    = "2021-10-07T07:24:06.126+02:00",
        type_val  = "decision",
        model_val = "messages-2.0",
        count_val = 1):
        """Return a default valid record """

        return {
            MESSAGE_ID_KEY : msg_val,
            TIMESTAMP_KEY  : ts_val,
            TYPE_KEY       : type_val,
            MODEL_KEY      : model_val,
            COUNT_KEY      : count_val,
            VARIANT_KEY    : { "text": "Have the courage to surrender to what happens next." },
            GIVENS_KEY     : {
                "app"                      : "#Mindful",
                "device"                   : "iPhone",
                "since_session_start"      : 323723.807,
                "since_midnight"           : 26646.122,
                "improve_version"          : 6000,
                "screen_height"            : 2532,
                "language"                 : "nb",
                "app_version"              : 7009,
                "country"                  : "NO",
                "device_version"           : 13003,
                "share_ratio"              : 0.0030716722831130028,
                "os"                       : "ios",
                "screen_width"             : 1170,
                "session_count"            : 1,
                "build_version"            : 2574000,
                "screen_pixels"            : 2962440,
                "carrier"                  : "Telenor",
                "since_born"               : 792140.915,
                "timezone"                 : 2,
                "os_version"               : 14007.001,
                "page"                     : 2462,
                "weekday"                  : 4.3084,
                "decision_count"           : 25,
                "since_last_session_start" : 792140.914,
                "shared" : {
                    "This moment is happening anyway. Stop trying to control it.": 1,
                    "There's no need to try so hard.": 1,
                    "Don't forget to love yourself.": 1,
                    "This is it.": 1
                },
            },
            RUNNERS_UP_KEY  : [ { "text": "You are safe." } ],
            SAMPLE_KEY      : { "text": "Remember when you wanted what you now have?" },
        }
    
    return __get_record