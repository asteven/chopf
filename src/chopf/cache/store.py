from ..exceptions import StoreKeyError


def meta_namespace_key_func(obj):
    """Create a key from the given object for use in a store."""
    try:
        name = obj.metadata.name
        namespace = getattr(obj.metadata, 'namespace', None)
        if namespace is not None:
            return f'{namespace}/{name}'
        else:
            return name
    except Exception as e:
        raise StoreKeyError(obj) from e


class Store:
    def __init__(self, key_func=None):
        if key_func is None:
            key_func = meta_namespace_key_func
        self.key_func = key_func
        self._items = {}

    def __repr__(self):
        keys = self.keys()
        return f'<Store {keys}>'

    def __setitem__(self, key, obj):
        # also support item assignment by explicit key and object
        self._items[key] = obj

    def __getitem__(self, key):
        # also make store subscriptable by key
        return self._items[key]

    def __delitem__(self, key):
        # also support item deletion by key
        del self._items[key]

    def add(self, obj):
        """Add the given item to the store."""
        key = self.key_func(obj)
        self[key] = obj

    def update(self, obj):
        """Update the given item in the store."""
        key = self.key_func(obj)
        self[key] = obj

    def delete(self, obj):
        """Delete the given item from the store."""
        key = self.key_func(obj)
        del self[key]

    def keys(self):
        """Return a list of keys of all items in the store."""
        return self._items.keys()

    def list(self):
        """Return a list of all items in the store."""
        return self._items.values()

    def get(self, obj):
        """Get a item from the store."""
        key = self.key_func(obj)
        return self[key]

    def replace(self, _list):
        """Replace all items in the store with those in the given list."""
        self._items.clear()
        for obj in _list:
            self.add(obj)

    def clear(self):
        """Remove all items from the store."""
        self._items.clear()
