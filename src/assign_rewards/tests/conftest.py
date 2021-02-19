from pytest_cases import fixture


@fixture
def listeners():
    listeners = [
        {
            "timestamp": "2020-01-01T00:00:00.000-05:00"
        },

        {
            "message_id":"37FADFEF-FB0E-471C-9E76-F12FE7016483",
            "context":  {
                "device_manufacturer":"Apple",
                "message"  : {
                    "ref"  : "Psalm 121:7",
                    "text" : "The Lord will keep you from all evil. He will keep your soul."
                }
            },

            "model"        : "themes-2.0",
            "reward_key"   : "Message Shared.id=Psalm 121:7/Green Water Ripples",
            "type"         : "decision",
            "timestamp"    : "2020-01-01T00:00:00.000-05:00",
            "history_id"   : "Ma0KwULnCXIc17c++XnY+a7Cz8M2BsyfCISkh0x9k/8=",
            "project_name" : "bible",
            "received_at"  : "2020-10-19T03:59:14.641Z",
            "variant"      : {
                "id": "Green Water Ripples",
            }
        }
    ]
    return listeners

@fixture
def decision_records():
    decision_records = [
        {
            "type"       : "decision", 
            "reward_key" : "rwkey_X",
            "timestamp"  : "2020-01-01T00:00:00.000-05:00",
            "history_id" : "aa4JDSiu3rKRjfEYaEVCFr9XTD3ZffsT4da/AwuBggU=",
            "message_id" : "decision_record_1"
        },

        {
            "type"       : "decision", 
            "reward_key" : "rwkey_X",
            "timestamp"  : "2020-01-01T01:00:00.000-05:00",
            "history_id" : "aa4JDSiu3rKRjfEYaEVCFr9XTD3ZffsT4da/AwuBggU=",
            "message_id" : "decision_record_2"
        },

        {
            "type"       : "decision", 
            "reward_key" : "rwkey_Y",
            "timestamp"  : "2020-01-01T02:00:00.000-05:00",
            "history_id" : "aa4JDSiu3rKRjfEYaEVCFr9XTD3ZffsT4da/AwuBggU=",
            "message_id" : "decision_record_3"
        }
    ]
    return decision_records

@fixture
def reward_records():
    reward_records = [
        {
            "rewards": {
                "rwkey_X"   : 1
            },
            "timestamp"    : "2020-01-01T00:30:00.000-05:00",
            "history_id"   : "MsXxZsakhenjEn3QYvjOP8t+MgNb3C81PliTuB7h5YE=",
            "type"         : "rewards",
            "message_id"   : "reward_record_1"
        },

        {
            "rewards": {
                "rwkey_X"   : 0.5
            },
            "timestamp"    : "2020-01-01T01:30:00.000-05:00",
            "history_id"   : "MsXxZsakhenjEn3QYvjOP8t+MgNb3C81PliTuB7h5YE=",
            "type"         : "rewards",
            "message_id"   : "reward_record_2"
        },

        {
            "rewards": {
                "rwkey_X"   : 0.3
            },
            "timestamp"    : "2020-01-01T02:30:00.000-05:00",
            "history_id"   : "MsXxZsakhenjEn3QYvjOP8t+MgNb3C81PliTuB7h5YE=",
            "type"         : "rewards",
            "message_id"   : "reward_record_3"
        }
    ]
    return reward_records


@fixture
def event_records():
    event_records = [
        {
            "type"       : "event",
            "event"      : "ev1",
            "properties" : { "value": .001 },
            "timestamp"  : "2020-01-01T00:15:00.000-05:00",
            "message_id" : "event_record_1"
        },

        {
            "type"       : "event",
            "event"      : "ev2",
            "properties" : { "value": .01 },
            "timestamp"  : "2020-01-01T01:15:00.000-05:00",
            "message_id" : "event_record_2"
        },

        {
            "type"       : "event",
            "event"      : "ev3",
            "properties" : { "value": .1 },
            "timestamp"  : "2020-01-01T02:45:00.000-05:00",
            "message_id" : "event_record_3"
        }
    ]
    return event_records


@fixture
def rewarded_records():
    expected = [
        {
            "type"       : 'decision',
            "reward_key" : 'rwkey_X',
            "timestamp"  : '2020-01-01T00:00:00.000-05:00',
            "history_id" : 'aa4JDSiu3rKRjfEYaEVCFr9XTD3ZffsT4da/AwuBggU=',
            "message_id" : 'decision_record_1',
            "reward"     : 1.511
        },

        {
            "type"       : 'decision',
            "reward_key" : 'rwkey_X',
            "timestamp"  : '2020-01-01T01:00:00.000-05:00',
            "history_id" : 'aa4JDSiu3rKRjfEYaEVCFr9XTD3ZffsT4da/AwuBggU=',
            "message_id" : 'decision_record_2',
            "reward"     : 0.81
        },

        {
            "type"       : 'decision',
            "reward_key" : 'rwkey_Y',
            "timestamp"  : '2020-01-01T02:00:00.000-05:00',
            "history_id" : 'aa4JDSiu3rKRjfEYaEVCFr9XTD3ZffsT4da/AwuBggU=',
            "message_id" : 'decision_record_3',
            "reward"     : 0.1
        }
    ]
    return expected


@fixture
def all_type_records(decision_records, reward_records, event_records):
    records = []
    records.extend(decision_records)
    records.extend(reward_records)
    records.extend(event_records)
    return records