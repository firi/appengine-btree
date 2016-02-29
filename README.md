# App Engine BTree

A counted BTree implementation for the Google App Engine Datastore
using ndb. A BTree is a balanced tree with a high (and configurable)
branching factor. When creating the tree you set the degree of the
tree, which determines the branching factor.

Three btree classes are provided in the btree module: BTree,
MultiBTree and MultiBTree2. All tree clasess use the same counted
btree implementation, and thus allow for fast finding of the N-th
entry and similar rank operations.

The BTree class implements a mapping from unique keys to
values. MultiBTree is a similar, but allows for multiple insertions of
the same key. Finally, MultiBTree2 adds an additional unique string
identifier to each (key, value) pair. This can be later used to find
that specific entry or to ensure that only a single (key, value) pair
is inserted for each unique identifier.

## Usage

Using a BTree is easy. The keys in the trees can be any sortable and
pickable Python object. Values can be any pickable Python object.

```
from btree import BTree

degree = 5
tree = BTree.get_or_create('tree', degree)
# Insert some values
for x in range(25):
    tree.insert(x, "value-%d" % x)
# Retrieve the first 10 items
items = tree[:10]
# Or get the last item
item = tree[-1:]
```

Note that all operations perform Datastore RPCs under the hood, and
every operation starts a new transaction. The btree module has a
special `perform_in_batch()` method which lets multiple operations on
one tree use the same trnasaction and caches the tree in
memory. Batching reduces Datastore RPCs and thus cost as well as
latency. You should batch whenever possible.

```
# It is save to get the tree entity outside the transaction,
# as it is immutable and never changes.
tree = BTree.get_by_id('tree')
def f():
    # All these inserts now use the same transaction
    # and much less RPCs.
    for x in range(25):
        tree.insert(x, "value-%d" % x)
    # Similar, getting the size uses the cached nodes and
    # performs no additional RPCs
    return tree.tree_size()
tree.perform_in_batch(f)
```

## Implementation Details

The BTree/MultiBTree/MultiBTree2 entity forms the root entity of the
entity group that contains the entire tree.  Each node in the tree is
serialized to a single entity in the App Engine datstore. The degree
of the tree must thus be chosen such that the total size of the node's
keys and values do not exceed the 1MB entity size limit. Each node
will hold a maximum of 2 * degree keys and values. Higher degrees
reduce the depth of the tree, and thus require fewer datastore
operation for most of the operations on the tree. As long as your keys
and values are small, a degree of around a few hundred should be fine.

Higher degrees do have slightly higher serialization costs, because
the entities themselves are larger. Although pickling is one of the
fastest serialization options available on App Engine Python, it
should still be kept in mind. Best is to try trees with varying
degrees on a real work load to get the right balance between the
amount of RPCs and serialization time.

The BTreeMulti2 implementation also stores an additional N entities
for indexing operations, where N is the number of items in the tree.

The main drawback of this btree module is that All entities in the
tree belong to a single entity group, which effectively limits the
write rate of the tree to about 1 write/second (in practice this is
higher, but less than an order of magnitude more). By using the batch
insert operations a higher effective insert rate can be achieved,
although some caution must be used to ensure that the AppEngine
transaction size limit of 10MB is not crossed. For example, if the
tree is very large, inserting 100 entries in a batch might touch about
100 nodes (each entry ends up in a separate node). If the nodes
themselves are large, the 10MB limit could be crossed.

### Production Use

MultiBTree2 is used for storing hundreds thousands of leaderboards,
with sizes varying from a few players to more than a couple of
million.

### Potential Improvements

* Allow more types to be used as identifiers in MultiBTree2. Anything
  that is a valid entity key should be usable as identifier.
* Slightly more deliberate and structured unittesting.

