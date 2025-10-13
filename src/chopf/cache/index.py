from . import Store


def index_by_namespace(obj):
    namespace = getattr(obj.metadata, 'namespace', None)
    return [namespace]


# Just an example, not used inside our code base.
def index_by_ip(obj):
    ips = []
    try:
        pod_ips = obj.status.podIPs
        if pod_ips:
            ips = [item.ip for item in pod_ips]
    except AttributeError as e:
        print(e)
        pass
    finally:
        return ips


class IndexView:
    def __init__(self, store, index_name, resource=None):
        self.store = store
        self.index_name = index_name
        self.resource = resource

    def __repr__(self):
        if self.resource:
            return f'<IndexView {self.resource.apiVersion}/{self.resource.kind} {self.index_name}>'
        else:
            return f'<IndexView {self.index_name}>'

    @property
    def _index(self):
        return self.store._indices[self.index_name]

    def __getitem__(self, key):
        return [self.store[k] for k in self._index[key]]

    def keys(self):
        return self._index.keys()

    def items(self):
        return {key: self[key] for key in self.keys()}

    def values(self):
        return [self[key] for key in self.keys()]


class Indexer(Store):
    def __init__(self, key_func=None, indexers=None):
        super().__init__(key_func=key_func)
        self._indexers = {}
        self._indices = {}
        self.add_indexers({'namespace': index_by_namespace})
        if indexers is not None:
            self.add_indexers(indexers)

    def __repr__(self):
        keys = self.keys()
        return f'<Indexer {keys}>'

    def __setitem__(self, key, obj):
        old = self._items.get(key, None)
        self._items[key] = obj
        self._update_indices(old, obj, key)

    def __getitem__(self, key):
        return self._items[key]

    def __delitem__(self, key):
        if key in self._items:
            obj = self._items.pop(key)
            self._update_indices(obj, None, key)

    def _update_single_index(self, name, old, new, key):
        index_func = self._indexers[name]
        old_index_values = []
        index_values = []
        if old:
            old_index_values = index_func(old)
        if new:
            index_values = index_func(new)

        # We optimize for the most common case where indexFunc
        # returns a single value which has not been changed.
        if (
            len(index_values) == 1
            and len(old_index_values) == 1
            and index_values[0] == old_index_values[0]
        ):
            return

        index = self._indices.get(name, None)
        if index is None:
            index = {}
            self._indices[name] = index

        for value in old_index_values:
            _set = index.get(value, None)
            if _set:
                _set.discard(key)

        for value in index_values:
            _set = index.get(value, None)
            if _set is None:
                _set = set()
                index[value] = _set
            _set.add(key)

    def _update_indices(self, old, new, key):
        for name in self._indexers:
            self._update_single_index(name, old, new, key)

    def add_indexers(self, indexers):
        old_keys = set(self._indexers.keys())
        new_keys = set(indexers.keys())
        if not old_keys.isdisjoint(new_keys):
            raise Exception('indexer conflict: %s' % old_keys.intersection(new_keys))
        for name, indexer in indexers.items():
            self._indexers[name] = indexer
            # Ensure indicies exist, even if store is empty.
            index = self._indices.get(name, None)
            if index is None:
                index = {}
                self._indices[name] = index
        # Re-index all items in the store.
        for key, value in self._items.items():
            for name in indexers.keys():
                self._update_single_index(name, None, value, key)

    def index(self, index_name):
        """Decorator that registers an indexer function with the given name."""

        def decorator(f):
            self.add_indexers({index_name: f})

        return decorator

    def get_index(self, index_name, resource=None):
        """Return a read-only view on a index."""
        return IndexView(self, index_name, resource=resource)

    def __index(self, index_name, obj):
        # TODO: is this needed?
        index_func = self._indexers[index_name]
        index = self._indices[index_name]
        index_values = index_func(obj)

        keys = []
        if len(index_values) == 1:
            value = index_values[0]
            keys = list(index[value])
        else:
            key_set = set()
            for value in index_values:
                for key in index[value]:
                    key_set.add(key)
            keys = list(key_set)

        return [self._items[key] for key in keys]
