from enum import Enum
from typing import Optional, Union, Tuple, List, Dict

from chainpy.autotask.eventabc import EventABC, CallAfter
from chainpy.autotask.utils import timestamp_msec, seq_log
from chainpy.eth.managers.eventhandler import DetectedEvent
from chainpy.eth.types.constant import ChainIndex
from chainpy.eth.types.hexbytes import EthHexBytes

from utils import get_events_from_file, write_events_to_file


EVENT_FILE_CACHE_PATH = "./eventcache"


class ChainMethodIndex(Enum):
    CROSS_WARP = 0x8000

    CROSS_DEPOSIT = 0x9001
    CROSS_REPAY = 0x9101
    CROSS_WITHDRAW = 0x9201
    CROSS_BORROW = 0x9301
    CROSS_SWAP = 0xa001
    CROSS_OPEN = 0xb001
    CROSS_END = 0xb101

    CROSS_BRIDGE = 0xc001
    CROSS_SWAP_BRIDGE = 0xc101


class ChainEventStatus(Enum):
    NONE = 0
    REQUESTED = 1
    EXECUTED = 2
    REVERTED = 3
    ACCEPTED = 4
    REJECTED = 5
    COMMITTED = 6
    ROLLBACKED = 7


RangesDict = Dict[ChainIndex, Tuple[int, int]]


def load_events_from_file(loaded_ranges: RangesDict, ranges: RangesDict) -> RangesDict:
    # load previous result from file
    if loaded_ranges is None:
        return ranges

    if set(loaded_ranges.keys()) != set(ranges.keys()):
        raise Exception("Invalid file_db contents: supported chain not match")

    ret_ranges = dict()
    for chain, to_block in loaded_ranges.items():
        unchecked_start, unchecked_end = ranges[chain][0], ranges[chain][1]
        loaded_start, loaded_end = loaded_ranges[chain][0], loaded_ranges[chain][1]
        if unchecked_start > loaded_end:
            raise Exception("Loaded monitor range does not cover: loaded_end({})".format(loaded_end))
        ret_ranges[chain] = (loaded_start, unchecked_end)
    return ret_ranges


def classify_events_by_request_id(events: List[DetectedEvent]) -> Dict[str, 'ChainEvent']:
    events_with_last_status = dict()
    for event in events:
        chain_event = ChainEvent.from_tuple(event.decoded_data, timestamp_msec())

        request_id = EthHexBytes(event.decoded_data[0][0][0], 4) + EthHexBytes(event.decoded_data[0][0][1], 4)
        request_id = request_id.hex()

        if request_id not in events_with_last_status:
            events_with_last_status[request_id] = chain_event
        elif events_with_last_status[request_id].status.value < chain_event.status.value:
            events_with_last_status[request_id] = chain_event

    return events_with_last_status


def erase_duplicate(events: List[DetectedEvent]) -> List[DetectedEvent]:
    events_dict = dict()
    for event in events:
        event_id = (event.decoded_data[0][0][0], event.decoded_data[0][0][1], event.decoded_data[0][1])
        if event_id not in events_dict:
            events_dict[event_id] = event
    return list(events_dict.values())


def erase_finalized_events(events: Dict[str, 'ChainEvent']) -> List['ChainEvent']:
    remove_key_list = list()
    for key, event in events.items():
        if event.status == ChainEventStatus.ROLLBACKED or event.status == ChainEventStatus.COMMITTED:
            remove_key_list.append(key)

    for key in remove_key_list:
        del events[key]

    return list(events.values())


def print_events(events: List[Union[DetectedEvent, 'ChainEvent']]):
    for event in events:
        if not isinstance(event, ChainEvent):
            event = ChainEvent.from_tuple(event.decoded_data, 0)
        print(event.summary())


class RelayerSubmitVote:
    """
    Data class for payload of relayer's voting.
    """

    def __init__(self, chain_event: 'ChainEvent', fail_flag: Optional[bool] = False):
        from random import Random
        self.__event = chain_event

        fail_flag = False

        # TODO remove before launching
        if fail_flag:
            rand = Random().randint(0, 1)
            self.__forced_fail = 0 if rand > 0 else 1
        else:
            self.__forced_fail = 0

        seq_log.info("[TxManager] fail({}), {}".format(self.__forced_fail, chain_event.summary()))

    def tuple(self):
        return self.__event.tuple(), self.__forced_fail


class ChainEvent(EventABC):
    """
    Data class for socket contract's event
    """
    CALL_DELAY_SEC = 5

    def __init__(self, event_tuple: tuple, time_lock: int, child_flag: bool = False):
        # event_tuple: (request_id_tuple), status_int, (inst_tuple), (action_params_tuple), sig_bytes
        super().__init__(event_tuple, time_lock)
        self.__child_flag = child_flag

    @classmethod
    def from_tuple(cls, event_tuple: tuple, time_lock: int, child_flag: bool = False):
        # request_id_tuple, status_int, inst_tuple, action_params_tuple, sigs_bytes = event_tuple[0]
        obj = cls(event_tuple[0], time_lock, child_flag)

        if obj.status == ChainEventStatus.REQUESTED:
            casting_type = ChainRequestEvent
        elif obj.status == ChainEventStatus.EXECUTED or obj.status == ChainEventStatus.REVERTED:
            casting_type = ChainExecutedRevertedEvent
        elif obj.status == ChainEventStatus.ACCEPTED or obj.status == ChainEventStatus.REJECTED:
            casting_type = ChainAcceptedRejectedEvent
        elif obj.status == ChainEventStatus.COMMITTED or obj.status == ChainEventStatus.ROLLBACKED:
            casting_type = ChainCommittedRollbackedEvent
        else:
            raise Exception("Not Supported")

        return obj.casting(casting_type)

    def tuple(self) -> tuple:
        return self.data

    def summary(self) -> str:
        request_id = (self.data[0][0], self.data[0][1])
        return "{}:{}".format(request_id, self.status)

    def build_transaction(self, *args):
        raise Exception("Not Implemented")

    def handle_fail_event(self, *args):
        raise Exception("Not Implemented")

    def handle_success_event(self, *args):
        raise Exception("Not Implemented")

    def handle_no_receipt_event(self, *args):
        raise Exception("Not Implemented")

    @staticmethod
    def bootstrap(detected_events: List[DetectedEvent], ranges: Dict[ChainIndex, Tuple[int, int]]) -> List[EventABC]:
        # load previous events from file
        loaded_events, loaded_ranges = get_events_from_file(EVENT_FILE_CACHE_PATH)

        # merge events, update ranges (including range validation)
        events_all = loaded_events + detected_events

        # erase duplicator
        events_all = erase_duplicate(events_all)

        # update ranges
        updated_ranges = load_events_from_file(loaded_ranges, ranges)

        # write event to file
        write_events_to_file(EVENT_FILE_CACHE_PATH, updated_ranges, events_all)
        # print_events(events_all)  # for dev

        # classify events by request_id
        event_with_last_status = classify_events_by_request_id(events_all)

        # remove duplicates
        erased_events = erase_finalized_events(event_with_last_status)

        seq_log.info("[Bootstrap] {} events was not handled in detail below".format(len(erased_events)))
        for event in erased_events:
            seq_log.info(event.summary())

        return erased_events

    # ************************************************************************************************* custom function
    def casting(self, casting_type: type, updated_status: ChainEventStatus = None, child_flag: bool = False):
        if not issubclass(casting_type, ChainEvent) and not issubclass(casting_type, CallAfter):
            raise Exception("{} class is not a child of {}".format(casting_type, ChainEvent))

        updated_event_tuple = self.tuple()
        if updated_status is not None:
            updated_status_list = list(updated_event_tuple)
            updated_status_list[1] = updated_status.value
            updated_event_tuple = tuple(updated_status_list)

        ret = casting_type(updated_event_tuple, self.time_lock, child_flag)
        ret.receipt_params = self.receipt_params
        return ret

    def check_inbound(self) -> bool:
        return self.origin_chain_index != ChainIndex.BIFROST

    def is_child_event(self) -> bool:
        return self.__child_flag

    def update_sigs(self, sigs: EthHexBytes):
        # data: (request_id_tuple), status_int, (inst_tuple), (action_params_tuple), sig_bytes
        data = list(self.tuple())
        data[4] = sigs
        self.data = tuple(data)

    @property
    def status(self) -> ChainEventStatus:
        # data: (request_id_tuple), status_int, (inst_tuple), (action_params_tuple), sig_bytes
        return ChainEventStatus(self.data[1])

    @property
    def origin_chain_index(self) -> ChainIndex:
        # data: (request_id_tuple), status_int, (inst_tuple), (action_params_tuple), sig_bytes
        return ChainIndex(self.data[0][0])

    @property
    def dst_chain_index(self) -> ChainIndex:
        # data: (request_id_tuple), status_int, (inst_tuple), (action_params_tuple), sig_bytes
        return ChainIndex(self.data[2][0])


class ChainRequestEvent(ChainEvent):
    def __init__(self, event_tuple: tuple, time_lock: int, child_flag: bool):
        super().__init__(event_tuple, time_lock, child_flag)
        if self.status != ChainEventStatus.REQUESTED:
            raise Exception("Event status not matches")

    @classmethod
    def from_tuple(cls, event_tuple: tuple, time_lock: int, self_gen: bool = False):
        return super().from_tuple(event_tuple, time_lock, self_gen)

    def build_transaction(self, entity_index: int = None) -> Tuple[ChainIndex, str, str, Union[tuple, list]]:
        """ A method to build a transaction which handles the event """
        # generate signature if it's needed
        sig = EthHexBytes(b'\00') if self.check_inbound() else EthHexBytes(b'\01')
        self.update_sigs(sig)
        return ChainIndex.BIFROST, "socket", "poll", RelayerSubmitVote(self, True).tuple()

    def handle_success_event(self, entity_index: int = None) -> 'ChainCallAfter':
        # clone and casting to "CallAfter"
        call_event: ChainCallAfter = self.casting(ChainCallAfter, child_flag=True)

        # prepare call parameter
        data = self.tuple()
        chain_index, seq = data[0][0], data[0][1]
        dst_chain_index = self.dst_chain_index

        call_chain = dst_chain_index if self.check_inbound() else ChainIndex(chain_index)

        # update call event
        call_event.time_lock = timestamp_msec() + ChainEvent.CALL_DELAY_SEC * 1000
        call_event.set_call_params(dst_chain_index, "socket", "get_request", [(call_chain.value, seq)])

        return call_event

    def handle_fail_event(self, entity_index: int = None) -> 'ChainExecutedRevertedEvent':
        updated_event = self.casting(ChainExecutedRevertedEvent, ChainEventStatus.REVERTED, child_flag=True)
        updated_event.receipt_params = None
        return updated_event

    def handle_no_receipt_event(self, entity_index: int = None) -> 'ChainExecutedRevertedEvent':
        return self.handle_fail_event()


class ChainExecutedRevertedEvent(ChainEvent):
    def __init__(self, event_tuple: tuple, time_lock: int, child_flag: bool):
        super().__init__(event_tuple, time_lock, child_flag)
        if self.status != ChainEventStatus.EXECUTED and self.status != ChainEventStatus.REVERTED:
            raise Exception("Event status not matches")

    @classmethod
    def from_tuple(cls, event_tuple: tuple, time_lock: int, self_gen: bool = False):
        return super().from_tuple(event_tuple, time_lock)

    def build_transaction(self, entity_index: int = None) -> Tuple[ChainIndex, str, str, Union[tuple, list]]:
        sig = EthHexBytes(b'\01') if self.check_inbound() else EthHexBytes(b'\00')
        self.update_sigs(sig)

        # for testing
        fail_control = False if self.dst_chain_index != ChainIndex.BIFROST else True
        return ChainIndex.BIFROST, "socket", "poll", RelayerSubmitVote(self, fail_control).tuple()

    def handle_success_event(self, entity_index: int = None) -> Optional['ChainCallAfter']:
        # clone and casting to "CallAfter"
        if self.check_inbound() and self.status == ChainEventStatus.EXECUTED:
            call_event: ChainCallAfter = self.casting(ChainCallAfter, child_flag=True)

            # prepare call parameter
            data = call_event.tuple()
            chain_index, seq = data[0][0], data[0][1]
            dst_chain_index = self.dst_chain_index

            # update call event
            call_event.time_lock = timestamp_msec() + ChainEvent.CALL_DELAY_SEC * 1000
            call_event.set_call_params(dst_chain_index, "socket", "get_request", [(dst_chain_index.value, seq)])

            return call_event
        else:
            return None

    def handle_fail_event(self, entity_index: int = None) -> 'ChainAcceptedRejectedEvent':
        next_status = ChainEventStatus(self.status.value + 2)
        updated_event: ChainAcceptedRejectedEvent = self.casting(ChainAcceptedRejectedEvent, next_status, child_flag=True)
        updated_event.receipt_params = None
        return updated_event

    def handle_no_receipt_event(self, entity_index: int = None) -> 'ChainAcceptedRejectedEvent':
        return self.handle_fail_event(entity_index)


class ChainAcceptedRejectedEvent(ChainEvent):
    def __init__(self, event_tuple: tuple, time_lock: int, child_flag: bool):
        super().__init__(event_tuple, time_lock, child_flag)
        if self.status != ChainEventStatus.ACCEPTED and self.status != ChainEventStatus.REJECTED:
            raise Exception("Event status not matches")

    @classmethod
    def from_tuple(cls, event_tuple: tuple, time_lock: int, self_gen: bool = False):
        return super().from_tuple(event_tuple, time_lock, self_gen)

    def build_transaction(self, entity_index: int = None) -> Tuple[ChainIndex, str, str, Union[tuple, list]]:
        dst_chain = self.origin_chain_index if self.check_inbound() else self.dst_chain_index
        if self.is_child_event():  # All relayer group vote
            sig = EthHexBytes(b'\00')
            self.update_sigs(sig)
            return dst_chain, "socket", "poll", RelayerSubmitVote(self, False).tuple()

        elif entity_index == 3:  # the relayer assigned by the contract votes only
            print("[Relayer] Im Zero Relayer")
            sig = EthHexBytes(b'\01' * 325)  # TODO signature aggregation
            self.update_sigs(sig)

            return dst_chain, "socket", "poll", RelayerSubmitVote(self, False).tuple()
        else:
            return ChainIndex.RESERVED, "", "", ()

    def handle_success_event(self, entity_index: int = None) -> None:
        return None

    def handle_fail_event(self, entity_index: int = None) -> None:
        # TODO logging with logLevel: "Critical"
        return None

    def handle_no_receipt_event(self, entity_index: int = None):
        # TODO logging with logLevel: "Critical"
        return None


class ChainCommittedRollbackedEvent(ChainEvent):
    def __init__(self, event_tuple: tuple, time_lock: int, child_flag: bool):
        super().__init__(event_tuple, time_lock, child_flag)
        if self.status != ChainEventStatus.COMMITTED and self.status != ChainEventStatus.ROLLBACKED:
            raise Exception("Event status not matches")

    @classmethod
    def from_tuple(cls, event_tuple: tuple, time_lock: int, self_gen: bool = False):
        return super().from_tuple(event_tuple, time_lock, self_gen)

    def build_transaction(self, entity_index: int = None) -> Tuple[ChainIndex, str, str, Union[tuple, list]]:
        return ChainIndex.RESERVED, "", "", []

    def handle_success_event(self, entity_index: int = None) -> None:
        return None

    def handle_fail_event(self, entity_index: int = None) -> None:
        return None

    def handle_no_receipt_event(self, entity_index: int = None) -> None:
        return None


class ChainCallAfter(CallAfter):
    QUORUM = 3

    def __init__(self, event_tuple: tuple, time_lock: int, child_flag: bool):
        super().__init__(event_tuple, time_lock)

    def build_transaction(self, entity_index: int = None) -> Tuple[ChainIndex, str, str, Union[tuple, list]]:
        return self.get_call_params()

    def post_process(self, result: tuple):
        vote_num = result[0][0][1]
        seq_log.info("[Call     ] {}, vote_num: {}".format(self.summary(), vote_num))

        origin_event = ChainEvent(self.data, self.time_lock)

        if vote_num < ChainCallAfter.QUORUM:
            status = origin_event.status
            if status == ChainEventStatus.REQUESTED:
                updated = origin_event.casting(ChainRequestEvent, child_flag=True).handle_fail_event()
            elif status == ChainEventStatus.EXECUTED or status == ChainEventStatus.REVERTED:
                updated = origin_event.casting(ChainExecutedRevertedEvent, child_flag=True).handle_fail_event()
            elif status == ChainEventStatus.ACCEPTED or status == ChainEventStatus.REJECTED:
                updated = origin_event.casting(ChainAcceptedRejectedEvent, child_flag=True).handle_fail_event()
            elif status == ChainEventStatus.COMMITTED or status == ChainEventStatus.ROLLBACKED:
                updated = origin_event.casting(ChainCommittedRollbackedEvent, child_flag=True).handle_fail_event()
            else:
                raise Exception("Not supported status")

            return updated
        return None

    def summary(self) -> str:
        return ChainEvent(self.data, self.time_lock).summary()


if __name__ == "__main__":
    loaded_events, loaded_ranges = get_events_from_file(EVENT_FILE_CACHE_PATH)
    print_events(loaded_events)
