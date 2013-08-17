# App Engine Btree

A counted BTree implementation for the Google App Engine Datastore.

## Usage

See btree.py for more details.

## Production Use

This module is used for thousands of leaderboards, varying from 10
players to a couple of million.

## TODO

* Allow more types to be used as identifiers in MultiBTree2. Anything
  that is a valid entity key should be usable as identifier.
* NDB async support. Datastore operations in the tree should make use
  of the ndb event loop, so the multiple trees can be used in
  parallel.

