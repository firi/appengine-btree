# App Engine Btree

A counted BTree implementation for the Google App Engine Datastore.

Three btree classes are provided in the btree module: BTree,
MultiBTree and MultiBTree2. All tree clasess use a counted btree
implementation, and thus allow for fast finding of the N-th entry and
similar rank operations.

The BTree class implements a mapping from unique keys to
values. MultiBTree is a similar, but allows for multiple insertions of
the same key. Finally, MultiBTree2 adds an additional unique string
identifier to each (key, value) pair. This can be later used to find
that specific entry or ensure that only a single (key, value) pair is
inserted for each unique identifier.

## Usage

See btree.py for more details.


## Production Use

MultiBTree2 is used for thousands of leaderboards, with sizes varying
from a few players to over a million.


## Potential Improvements

* Allow more types to be used as identifiers in MultiBTree2. Anything
  that is a valid entity key should be usable as identifier.
* NDB async support. Datastore operations in the tree should make use
  of the ndb event loop, so the multiple trees can be used in
  parallel.

