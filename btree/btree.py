"""
A counted BTree implementation for the Google App Engine datastore.

Three BTree classes are provided, one which acts as a normal sorted
map and and another two that act as a sorted multimap. All
implementations use a counted BTree, and thus allow indexed access
into their elements. The three implementations are BTree, BTreeMulti
and BTreeMulti2. See the comments of the class for the specific use
cases of the different trees.

The keys in the trees can be any sortable and pickable python
object. Values can be any pickable python object.

A BTree is a balanced tree with a high (and configurable) branching
factor. When creating the tree, the user sets the degree of the tree,
which determines the branching factor.

Each node in the tree is serialized to a single entity in the App
Engine datstore. The degree must thus be chosen such that the total
size of the node's keys and values do not exceed the 1MB entity size
limit. Each node will hold a maximum of 2 * degree keys and
values. The BTreeMulti2 implementation also stores an additional 2 *
degree entities for indexing operations.

Higher degrees reduce the depth of the tree, and thus require fewer
datastore operations for most of the functionality of the tree. Larger
nodes do have a higher serialization cost.

All entities in the tree belong to a single entity group, which
effectively limits the write rate of the tree to about 1
write/second. Using the batch insert operations a higher effective
insert rate can be achieved, although some caution must be used to
ensure that the AppEngine transaction size limit of 10MB is not
crossed. For example, if the tree is very large, inserting 100 entries
in a batch might touch about 100 nodes (each entry ends up in a
separate node). If the nodes themselves are large, the 10MB limit
could be crossed.

Note that all methods of the tree will open a new transaction and send
RPCs to perform the requested method.

Multiple operations on a single tree can be easily batched using the
perform_in_batch() method.. Batching opens a single transaction for
all operations and caches results in memory, thus reducing datastore
operations, latency and cost.
"""
from google.appengine.ext import ndb
import internal

__author__ = "Tijmen Roberti"
__license__ = "MIT"
__all__ = ['BTree', 'MultiBTree', 'MultiBTree2']


def batch_operation(func):
    """
    Decorator to wrap the instance functions of the various trees in a
    call to perform_in_batch()
    """
    import functools
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        def f():
            return func(self, *args, **kwargs)
        return self.perform_in_batch(f)
    return wrapper


class _BTreeBase(internal._BTreeBase):
    """
    Contains all operations that are common to all trees.
    """

    @batch_operation
    def get_by_index(self, index):
        """
        Returns the item at the given index. Raises an IndexError if
        the index is out of bounds.
        """
        return self._get_by_index(n)

    @batch_operation
    def get_range(self, a, b):
        """
        Returns a list of items pairs that are on the indexes in the
        interval [a, b). Identical to applying the slice operator (with
        caveats, see __getitem__ for details).
        """
        return self[a:b]

    @batch_operation
    def index(self, key):
        """
        Returns the index of the entry with the given key in the tree.

        Raises:
          IndexError: if the key does not exist in the tree.
        """
        i = self._left_index_of_key(key)
        if i == -1:
            raise IndexError("Key %s not found in the tree." % key)
        return i

    @batch_operation
    def lower_bound(self, key):
        """
        Returns the index of the first item whose key is not smaller
        than the given |key|.
        """
        return self._lower_bound_index(key)

    @batch_operation
    def upper_bound(self, key):
        """
        Returns the index of the first item whose key is strictly
        greater than |key|.
        """
        return self._upper_bound_index(key)

    @batch_operation
    def pop(self, index):
        """
        Removes and returns the item tuple at the given |index|.

        Raises:
           IndexError: If the index is out of bounds.
        """
        return self._delete_index(index)

    @batch_operation
    def tree_size(self):
        """
        Returns the size of the tree. This operations runs in time
        linear to the degree of the tree, so it is preferable to cache
        this value when possible.
        """
        return self._size()


    def perform_in_batch(self, func):
        """
        Executes multiple operations on this tree in a single batch
        operation. Batching operations improves caching and reduces
        datastore calls (and thus both cost and latency). The function
        accepts as single argument |func|, which must be a function
        with no arguments. Calls to perform_in_batch() can be
        nested.

        This function also starts a transaction if one has not yet
        started.

        Example:

        tree = ...
        def f():
            tree.update(some_keys_and_values)
            tree.remove(a_key)
        tree.perform_in_batch(f)
        """
        return self._batch_operations(func)


    @batch_operation
    def __getitem__(self, index):
        """
        Returns the item at the given index. If a slice is provided, a
        list containing all items in the range are provided. Note that
        the slice arguments are more limited than those of a general
        list. Only positive indices and step values are allowed for
        slices. A negative index is allowed for single items.

        Raises a ValueError if the index is out of range, or when
        negative values are used.
        """
        if isinstance(index, slice):
            start, stop, step = index.indices(self.tree_size())
            if step != 1:
                # User can implement this themselves, as 'under the
                # hood' the full range gets retrieved anyway, so there
                # is no performance benefit.
                raise ValueError("Stepping in a slice is not supported")
            return self._get_by_index_range(start_index=start, num=stop - start)
        else:
            return self._get_by_index(index)


    @batch_operation
    def __contains__(self, key):
        return self._get_by_key(key) is not None


class BTree(_BTreeBase):
    """
    A counted BTree datastructure, which acts as a set.

    The methods specified in this class are in addition to the ones
    described above.
    """
    @staticmethod
    def create(key_name, minimum_degree, parent=None):
        """
        Create a new BTree instance with the given |key_name|.
        """
        tree = BTree(id=key_name, parent=parent)
        tree._initialize(minimum_degree)
        return tree

    @batch_operation
    def insert(self, key, value):
        """
        Inserts a new value in the btree for the given key.
        Any existing value for that key will be overwritten.
        """
        self._insert(key, value, None, allow_duplicates=False)

    @batch_operation
    def update(self, iterable):
        """
        Inserts multiple key, value pairs in the tree. Any iterable
        that yields (key, value) pairs can be used as input for this
        function.
        """
        for (key, value) in iterable:
            self._insert(key, value, None, allow_duplicates=False)

    @batch_operation
    def get(self, key):
        """
        Returns:
            The value that corresponds to the given key,
            or None if no such value exists.
        """
        return self._get_by_key(key)

    @batch_operation
    def remove(self, key):
        """
        Remove the entry with the given |key|.
        """
        self._delete_key(key)


class MultiBTree(_BTreeBase):
    """
    A counted BTree datastructure, which accepts multiple identical
    keys.

    If the items need to be uniquely identifable, use MultiBTree2.
    """
    @staticmethod
    def create(key_name, minimum_degree, parent=None):
        """
        Create a new BTree instance with the given |key_name|.
        """
        tree = MultiBTree(id=key_name, parent=parent)
        tree._initialize(minimum_degree)
        return tree

    @batch_operation
    def insert(self, key, value):
        """
        Inserts a new value in the btree with the given key. Multiple
        identical keys are allowed, and are ordered in insertion
        order.
        """
        self._insert(key, value, None, allow_duplicates=True)

    @batch_operation
    def update(self, iterable):
        """
        Inserts multiple key, value pairs in the tree. Any iterable
        that yields (key, value) pairs can be used as input for this
        function.
        """
        for (key, value) in iterable:
            self._insert(key, value, None, allow_duplicates=True)

    @batch_operation
    def count(self, key):
        """
        Counts the number of occurrences of |key|.
        """
        return (self._right_index_of_key(key) - self._left_index_of_key(key))

    @batch_operation
    def get_all(self, key):
        """
        Returns a list with all (key, value) pairs stored in the tree
        that match the given |key|.
        """
        return self._get_all_by_key(key)

    @batch_operation
    def index_left(self, key):
        """
        Returns the index of the first item with the given key.

        Raises:
          IndexError: If the key does not exist in the tree.
        """
        return self.index(key)

    @batch_operation
    def index_right(self, key):
        """
        Returns the index of the item after the last entry with the
        given |key|.

        Raises:
          IndexError: If the key does not exist in the tree.
        """
        i = self._right_index_of_key(key)
        if i == -1:
            raise IndexError("Key %s not found in the tree." % key)
        return i

    @batch_operation
    def remove_all(self, key):
        """
        Removes all entries with the given |key|.
        """
        self._delete_key_all(key)


class MultiBTree2(_BTreeBase):
    """
    Same as the other multi btree, but each item in the tree is
    accompanied by a unique user-provided identifier. This identifier
    can be used for various purposes, such as deleting or retrieving
    that specific item.

    Also it can be used to ensure uniqueness when inserting items, as
    only one item with a given identifier is in the tree at any time.

    The identifiers do come at a slight performance and storage
    cost. Identifiers are indexed in the datastore, so that gives an
    additional 2 extra write operations per insert/delete. Also,
    insert operations will perform an extra datastore read to check if
    the identifier is already used in the tree.

    Obviously, storage costs are also increased, as the identifier is
    stored with each key, value pair.
    """
    @staticmethod
    def create(key_name, minimum_degree, parent=None):
        """
        Create a new BTree instance with the given |key_name|.
        """
        tree = MultiBTree2(id=key_name, parent=parent)
        tree._initialize(minimum_degree)
        return tree

    @batch_operation
    def insert(self, key, value, identifier):
        """
        Inserts a new value in the btree with the given key and unique
        identifier. Multiple identical keys are allowed, and are
        ordered in insertion order.

        The parameter |identifier| must be a string that uniquely
        identifies this key/value pair, which can later be used to
        remove this pair. If an entry already exists in the tree with
        the given |identifier|, it will be replaced by the new key,
        value pair.

        Providing the |key| as the |identifier| effectively turns this
        into a normal BTree, but with extra overhead caused by the
        identifier querying. In those cases you are better off using a
        BTree.
        """
        if identifier is not None:
            self._insert(key, value, identifier, allow_duplicates=True)
        else:
            raise ValueError("Invalid identifier: %s" % identifier)

    @batch_operation
    def update(self, iterable):
        """
        Inserts multiple key, value, identifier tuples in the
        tree. Any iterable that yields (key, value, identifier) tuples
        can be used as input for this function.
        """
        keys, values, identifiers = zip(*iterable)
        for id in identifiers:
            if id is None:
                raise ValueError("Identifiers cannot be None")
        self._populate_identifier_cache(identifiers)
        import itertools
        for (key, value, id) in itertools.izip(keys, values, identifiers):
            self._insert(key, value, id, allow_duplicates=True)

    @batch_operation
    def count(self, key):
        """
        Counts the number of occurrences of |key|.
        """
        return (self._right_index_of_key(key) - self._left_index_of_key(key))

    @batch_operation
    def get_all(self, key):
        """
        Returns a list with all items, each a (key, value, identifier)
        tuple, that match the given key in this tree.
        """
        return self._get_all_by_key(key)

    @batch_operation
    def get_by_identifier(self, identifier):
        """
        Returns the (key, value, identifier) item that corresponds to
        the given unique identifier. Returns None is no such item
        exists.
        """
        return self._get_by_identifier(identifier)

    @batch_operation
    def index_left(self, key):
        """
        Identical to a call to index(key).
        """
        return self.index(key)

    @batch_operation
    def index_right(self, key):
        """
        Returns the index of the item after the last entry with the
        given |key|.
        """
        i = self._right_index_of_key(key)
        if i == -1:
            raise IndexError("Key %s not found in the tree." % key)
        return i

    @batch_operation
    def remove_all(self, key):
        """
        Removes all entries with the given |key|.
        """
        self._delete_key_all(key)

    @batch_operation
    def remove_by_identifier(self, identifier):
        """
        Removes the entry with the given |identifier|.
        """
        self._delete_identifier(identifier)

