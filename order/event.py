import json


class Event:
    def __init__(self, event_type, data):
        self.event_type = event_type
        self.data = data

    def to_json(self):
        return json.dumps({
            "event_type": self.event_type,
            "data": self.data
        })

    @staticmethod
    def from_json(self, json_str):
        event_dict = json.loads(json_str)
        return Event(event_dict["event_type"], event_dict["data"])

