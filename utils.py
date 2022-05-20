import json
import os
from datetime import datetime
from typing import Dict, Tuple, List

from chainpy.eth.managers.eventhandler import DetectedEvent
from chainpy.eth.types.constant import ChainIndex


def create_directory(path_: str):
    if not os.path.exists(path_):
        os.makedirs(path_)


def write_events_to_file(directory: str, ranges: Dict[ChainIndex, Tuple[int, int]], events: List[DetectedEvent]):
    create_directory(directory)

    directory = directory if directory.endswith("/") else directory + "/"
    file_name = datetime.now().strftime("%y.%m.%d-%H:%m:%S") + ".json"

    serialized_ranges = dict()
    for key, value in ranges.items():
        serialized_ranges[key.name] = value

    serialized_events = list()
    for event in events:
        event_json = {
            "topic": event.topic.hex(),
            "event_name": event.event_name,
            "decoded_data": str(event.decoded_data),
        }
        serialized_events.append(event_json)

    data = {
        "ranges": serialized_ranges,
        "events": serialized_events
    }

    # export new file
    with open(directory + file_name, "w") as f:
        json.dump(data, f)


def get_events_from_file(directory: str) -> (List[DetectedEvent], Dict[ChainIndex, Tuple[int, int]]):
    directory = directory if directory.endswith("/") else directory + "/"

    create_directory(directory)

    file_names = os.listdir(directory)
    latest_file_index = None
    if len(file_names) > 1:
        file_names_list = [file_name.replace("-", ".").replace(":", ".").replace(".json", "").split(".") for file_name in file_names]
        file_name_ints = list()
        for file_name_list in file_names_list:
            file_name_int = int("".join([item for item in file_name_list]))
            file_name_ints.append(file_name_int)
        latest_file_index = file_name_ints.index(max(file_name_ints))

    if latest_file_index is None:
        return [], None

    with open(directory + file_names[latest_file_index], "r") as json_data:
        file_db = json.load(json_data)
        ranges = dict()
        for key, value in file_db["ranges"].items():
            chain = eval("ChainIndex." + key)
            ranges[chain] = value

        detected_events = list()
        for event in file_db["events"]:
            event_obj = DetectedEvent(event["event_name"], event["topic"], eval(event["decoded_data"]))
            detected_events.append(event_obj)

        return detected_events, ranges


if __name__ == "__main__":
    result = get_events_from_file("./eventcache")
    print(result)
