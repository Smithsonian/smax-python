from abc import ABC, abstractmethod
from collections import namedtuple


# Named tuple for data and metadata returned from smax.
SmaxData = namedtuple("SmaxData", "data type dim date source seq")


class SmaxClient(ABC):

    def __init__(self, *args, **kwargs):
        # All child classes will call their smax_connect_to() when constructed.
        self._client = self.smax_connect_to(*args, **kwargs)

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        return self.smax_disconnect()

    @abstractmethod
    def smax_connect_to(self, *args, **kwargs):
        pass

    @abstractmethod
    def smax_disconnect(self):
        pass

    @abstractmethod
    def smax_pull(self, table, key):
        pass

    @abstractmethod
    def smax_share(self, table, key, value):
        pass

    @abstractmethod
    def smax_lazy_pull(self, table, key, value):
        pass

    @abstractmethod
    def smax_lazy_end(self, table, key):
        pass

    @abstractmethod
    def smax_subscribe(self, pattern, key):
        pass

    @abstractmethod
    def smax_unsubscribe(self, pattern, key):
        pass

    @abstractmethod
    def smax_wait_on_subscribed(self, table, key):
        pass

    @abstractmethod
    def smax_wait_on_subscribed_group(self, match_table, changed_key):
        pass

    @abstractmethod
    def smax_wait_on_subscribed_var(self, match_key, changed_table):
        pass

    @abstractmethod
    def smax_wait_on_any_subscribed(self, changed_table, changed_key):
        pass

    @abstractmethod
    def smax_release_waits(self, pattern, key):
        pass

    @abstractmethod
    def smax_queue(self, table, key, value, meta):
        pass

    @abstractmethod
    def smax_queue_callback(self, function, *args):
        pass

    @abstractmethod
    def smax_create_sync_point(self):
        pass

    @abstractmethod
    def smax_sync(self, sync_point, timeout_millis):
        pass

    @abstractmethod
    def smax_wait_queue_complete(self, timeout_millis):
        pass

    @abstractmethod
    def smax_set_description(self, table, key, description):
        pass

    @abstractmethod
    def smax_get_description(self, table, key):
        pass

    @abstractmethod
    def smax_set_units(self, table, key, unit):
        pass

    @abstractmethod
    def smax_get_units(self, table, key, unit):
        pass

    @abstractmethod
    def smax_set_coordinate_system(self, table, key, coordinate_system):
        pass

    @abstractmethod
    def smax_get_coordinate_system(self, table, key):
        pass

    @abstractmethod
    def smax_create_coordinate_system(self, n_axis):
        pass

    @abstractmethod
    def smax_push_meta(self, meta, table, key, value):
        pass

    @abstractmethod
    def smax_pull_meta(self, table, key):
        pass

    @abstractmethod
    def smax_set_resilient(self, value):
        pass

    @abstractmethod
    def smax_is_resilient(self):
        pass
