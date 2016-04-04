"""
Tests for the BTrees.
"""
import logging
import unittest
from google.appengine.ext import ndb
from google.appengine.ext import testbed
from google.appengine.datastore import datastore_stub_util

from . import BTree, MultiBTree, MultiBTree2

import internal


class BTreeTestBase(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=0)
        self.testbed.init_datastore_v3_stub(consistency_policy=self.policy)
        self.testbed.init_memcache_stub()
        # Silences the logging messages during the tests
        ndb.add_flow_exception(ValueError)
        ndb.add_flow_exception(IndexError)

    def tearDown(self):
        self.testbed.deactivate()


# helper functions
def issorted(l):
    return all(l[i] <= l[i+1] for i in xrange(len(l)-1))

def walk_items(tree):
    return tree[:]

def walk_keys(tree):
    return [item[0] for item in walk_items(tree)]


class BTreeTest(BTreeTestBase):
    def validate_tree(self, tree):
        """
        Checks if the tree is still valid. That is the ordering is still
        correct.
        """
        items = tree[:]
        keys = [item[0] for item in items]
        self.assertTrue(issorted(keys))


    def validate_indices(self, tree):
        """
        Checks to see if every item in the tree also has a matching index.
        """
        items = tree[:]
        for item in items:
            key = ndb.Key(MultiBTree2, "tree", internal._BTreeIndex, item[2])
            index = key.get()
            self.assertIsNotNone(index)
            self.assertEqual(index.tree_key, item[0])
            self.assertEqual(index.tree_value, item[1])


    def validate_empty_tree(self, tree):
        """
        An empty tree should consist of only two entities, the tree
        itself and the root node. No indices, no other nodes.
        """
        first = tree.key
        last = ndb.Key(first.kind(), first.id() + u"\ufffd")
        q = ndb.Query(ancestor=first)
        q = q.filter(tree.__class__.key < last)
        keys = list(q.iter(keys_only=True))
        self.assertEqual(keys, [first, tree._make_node_key("root")])


    def test_create(self):
        tree = BTree.create("tree", 2)
        self.assertEqual(tree.tree_size(), 0)
        self.assertRaises(ValueError, BTree.create, None, 2)
        self.validate_empty_tree(tree)

    def test_create_invalid_degree(self):
        self.assertRaises(ValueError, BTree.create, "tree", 1)


    def test_get_or_create(self):
        """
        Tests get_or_create function.
        """
        # BTree
        tree = BTree.get_or_create("tree", 2)
        self.assertTrue(isinstance(tree, BTree))
        self.assertEqual(tree.degree, 2)
        self.validate_empty_tree(tree)
        # MultiBTree
        mtree = MultiBTree.get_or_create("mtree", 5)
        self.assertTrue(isinstance(mtree, MultiBTree))
        self.assertEqual(mtree.degree, 5)
        self.validate_empty_tree(mtree)
        # MultiBTree2
        mtree2 = MultiBTree2.get_or_create("mtree2", 10)
        self.assertTrue(isinstance(mtree2, MultiBTree2))
        self.assertEqual(mtree2.degree, 10)
        self.validate_empty_tree(mtree2)
        # Test with a parent entity
        parent = ndb.Key('Parent', 'test')
        tree_with_parent = BTree.get_or_create('tree-with-parent', 2,
                                               parent=parent)
        self.assertEqual(tree_with_parent.key,
                         ndb.Key('Parent', 'test', 'BTree', 'tree-with-parent'))
        # Test get inside a transaction.
        def txn():
            return BTree.get_or_create("tree", 2)
        tree_txn = ndb.transaction(txn)
        self.assertEqual(tree, tree_txn)
        # Test create inside a transaction
        def txn():
            return BTree.get_or_create("tree-new", 2, parent=parent)
        tree_new = ndb.transaction(txn)
        self.assertIsNotNone(tree_new)
        # Test invalid degree
        self.assertRaises(ValueError, BTree.get_or_create, "tree-invalid", 1)


    def test_insert_full_root(self):
        t = 5
        tree = BTree.create("tree", 5)
        for x in reversed(range(2 * t - 1)):
            tree.insert(x, str(x))
        self.assertEqual(tree.tree_size(), 2 * t - 1)

    def test_insert_multiple(self):
        """
        Inserts a random sequence of keys, tests ordering.
        """
        tree = BTree.create("tree", 2)
        seq = [10, 8, 13, 11, 12, 2, 1, 14, 5, 0, 3, 7, 4, 9, 6]
        for x in seq:
            tree.insert(x, str(x))
        self.assertEqual(sorted(seq), walk_keys(tree))
        self.assertEqual(tree.tree_size(), len(seq))

    def test_insert_duplicate_keys_replaced(self):
        tree = BTree.create("tree", 3)
        seq = [10, 8, 13, 11, 12, 2, 1, 14, 5, 0, 3, 7, 4, 9, 6]
        for x in seq:
            tree.insert(x, str(x))
        for x in seq:
            tree.insert(x, str(2 * x))
        self.assertEqual([(x, str(2 * x)) for x in sorted(seq)],
                         walk_items(tree))
        self.assertEqual(tree.tree_size(), len(seq))

    def test_insert_delete_single_item(self):
        """Insert and delete a single item."""
        tree = BTree.create("tree", 3)
        tree.insert(1, "1")
        tree.remove(1)
        self.assertEqual([], walk_keys(tree))
        self.validate_empty_tree(tree)

    def test_delete_nonexisting_items(self):
        """Insert and delete a single item."""
        tree = BTree.create("tree", 3)
        out = tree.remove(234)
        self.assertIsNone(out)

    def test_insert_two_delete_one(self):
        tree = BTree.create("tree", 3)
        tree.insert(1, "1")
        tree.insert(2, "2")
        tree.remove(1)
        self.assertEqual([2], walk_keys(tree))
        tree = BTree.create("tree", 3)
        tree.insert(1, "1")
        tree.insert(2, "2")
        tree.remove(2)
        self.assertEqual([1], walk_keys(tree))


    def test_insert_and_delete_all(self):
        """Inserts and removes all items, resulting in an empty tree"""
        tree = BTree.create("tree", 3)
        seq = list(range(20))
        for x in seq:
            tree.insert(x, str(x))
        for x in seq:
            tree.remove(x)
        self.assertEqual([], walk_keys(tree))
        self.assertEqual(len([]), tree.tree_size())
        self.validate_empty_tree(tree)


    def test_delete_scenarios(self):
        """Tests various delete scenarios."""
        tree = BTree.create("tree", 2)
        seq = list(range(25))
        for x in seq:
            tree.insert(x, str(x))
        self.validate_tree(tree)
        # Triggers taking a key from left sibling
        tree.remove(7)
        seq.remove(7)
        self.assertEqual(seq, walk_keys(tree))
        # Triggers merge right
        tree.remove(19)
        seq.remove(19)
        self.assertEqual(seq, walk_keys(tree))
        # Triggers taking a key from right sibling
        tree.remove(15)
        seq.remove(15)
        self.assertEqual(seq, walk_keys(tree))
        # Delete all
        for x in list(seq):
            tree.remove(x)
            seq.remove(x)
            self.assertEqual(seq, walk_keys(tree))
        self.validate_empty_tree(tree)


    def test_delete_subtree(self):
        """Tests find predecessor/successor for subtrees"""
        tree = BTree.create("tree", 3)
        seq = list(range(25))
        for x in seq:
            tree.insert(x, str(x))
        self.assertEqual(seq, walk_keys(tree))
        tree.remove(8)
        seq.remove(8)
        self.assertEqual(seq, walk_keys(tree))
        tree.remove(9)
        seq.remove(9)
        self.assertEqual(seq, walk_keys(tree))
        self.assertEqual(len(seq), tree.tree_size())


    def test_get_by_index(self):
        tree = BTree.create("tree", 2)
        items = [(x, str(x)) for x in range(50)]
        for x in items:
            tree.insert(*x)
        for i in range(len(items)):
            self.assertEqual(items[i], tree[i])
        self.assertRaises(IndexError, tree.__getitem__, len(items) + 1)
        self.assertEqual(items, tree[0:tree.tree_size()])
        self.assertEqual(tree[-1], tree[tree.tree_size() - 1])
        self.assertRaises(IndexError, tree.__getitem__, -(len(items) + 1))

        self.assertEqual(tree.get_by_index(0), tree[0])
        with self.assertRaises(ValueError):
            tree[::2]
        self.assertEqual(tree.get_range(5, 10), tree[5:10])


    def test_get_range(self):
        tree = BTree.create("tree", 2)
        items = [(x, str(x)) for x in range(50)]
        for x in items:
            tree.insert(*x)

        for x in [0, 1, 2, 3, 5, 11, 17, 31, 50]:
            for i in range(len(items)):
                self.assertEqual(items[i:i+x], tree[i:i+x])


    def test_get_by_key(self):
        """Tests get by key for a normal tree"""
        tree = BTree.create("tree", 3)
        self.assertIsNone(tree.get(123))
        seq = [10, 8, 13, 11, 12, 2, 1, 14, 5, 0, 3, 7, 4, 9, 6]
        for x in seq:
            tree.insert(x, str(x))
        for x in seq:
            tree.insert(x, str(2 * x))
        for x in seq:
            self.assertEqual((x, str(2 * x)), tree.get(x))

        other = set(range(-10, 20)) - set(seq)
        for x in other:
            self.assertEqual(tree.get(x), None)


    def test_delete_by_index_cases(self):
        """Tests various delete scenarios when deleting by index"""
        tree = BTree.create("tree", 3)
        seq = [(x, str(x)) for x in range(20)]
        for item in seq:
            tree.insert(*item)

        # Triggers two merges, one internal, one in leaf and
        # a empty root that is deleted.
        self.assertEqual(tree.pop(0), seq.pop(0))
        self.assertEqual(seq, tree[:])
        # Triggers taking a key from left sibling
        self.assertEqual(tree.pop(5), seq.pop(5))
        self.assertEqual(seq, tree[:])
        # Triggers taking a key from right sibling
        self.assertEqual(tree.pop(10), seq.pop(10))
        self.assertEqual(seq, tree[:])
        # Tests merging with left sibling
        self.assertEqual(tree.pop(6), seq.pop(6))
        self.assertEqual(seq, tree[:])
        # Tests find predecessor
        self.assertEqual(tree.pop(1), seq.pop(1))
        self.assertEqual(seq, tree[:])
        # Tests find successor
        self.assertEqual(tree.pop(0), seq.pop(0))
        self.assertEqual(seq, tree[:])


    def test_with_zero_key(self):
        """
        Test tree with a key that is 0.
        """
        tree = BTree.create("tree", 3)
        seq = list(range(-5, 5))
        for x in seq:
            tree.insert(x, str(x))
        self.assertEqual(seq, walk_keys(tree))


    def test_delete_by_index_subtree(self):
        """
        Tests find predecessor/successor for subtree when deleting by
        index.
        """
        tree = BTree.create("tree", 2)
        seq = list(range(12))
        for x in seq:
            tree.insert(x, str(x))
        self.assertEqual(seq, walk_keys(tree))
        tree.pop(8)
        seq.pop(8)
        self.assertEqual(seq, walk_keys(tree))
        tree.pop(8)
        seq.pop(8)
        self.assertEqual(seq, walk_keys(tree))
        self.assertEqual(len(seq), tree.tree_size())


    def test_insert_stable(self):
        """Tests if the tree is stable under insert for identical keys"""
        tree = MultiBTree.create("tree", 3)
        items = [(2, str(x)) for x in range(20)]
        for k, v in items:
            tree.insert(k, v)
        self.assertEqual(list(items), walk_items(tree))
        self.assertEqual(tree.tree_size(), len(items))

    def test_insert_duplicate_keys(self):
        """
        Insert the same sequence twice in a multitree.
        """
        tree = MultiBTree.create("tree", 3)
        seq = [10, 8, 13, 11, 12, 2, 1, 14, 5, 0, 3, 7, 4, 9, 6]
        for x in seq:
            tree.insert(x, str(x))
        for x in seq:
            tree.insert(x, str(x))
        self.assertEqual(sorted(seq + seq), walk_keys(tree))

    def test_get_by_key_multi(self):
        """Tests get_all for a multitree"""
        tree = MultiBTree.create("tree", 3)
        seq = [(x, str(x)) for x in range(21)]
        seq.remove((5, "5"))
        for x in seq:
            self.assertEqual([], tree.get_all(x[0]))
        for x in seq:
            tree.insert(*x)
        items = [(5, str(x)) for x in range(11)]
        for item in items:
            tree.insert(*item)

        self.assertEqual(items, tree.get_all(5))
        for x in seq:
            self.assertEqual([x], tree.get_all(x[0]))

        tree = MultiBTree.create("tree-all", 3)
        items = [(2, str(x)) for x in range(40)]
        for item in items:
            tree.insert(*item)
        self.assertEqual(items, tree.get_all(2))

        tree3 = MultiBTree2.create("tree-3", 3)
        items = [(3, str(x), str(x)) for x in range(10)]
        tree3.update(items)
        self.assertEqual(items, tree3.get_all(3))


    def test_delete_multitree(self):
        """Tests delete all for multitrees"""
        tree = MultiBTree.create("tree", 3)
        for x in range(10):
            tree.insert(2, "2")
        tree.remove_all(2)
        tree.remove_all(1234)   # remove non-existent key

        self.assertEqual([], walk_items(tree))
        seq = list(range(10))
        for x in seq:
            tree.insert(x, str(x))
            tree.insert(100, "100")
        tree.remove_all(100)
        self.assertEqual(seq, walk_keys(tree))
        self.assertEqual(len(seq), tree.tree_size())

        for x in seq:
            tree.remove_all(x)
        self.assertEqual(0, tree.tree_size())
        self.validate_empty_tree(tree)


    def test_delete_stable(self):
        """Tests the stability of inserts for multitries while deleting."""
        tree = MultiBTree.create("tree", 3)
        seq = [(x, str(x)) for x in range(21)]
        seq.remove((5, "5"))
        tree.update(seq)
        items = [(5, str(x)) for x in range(11)]
        tree.update(items)
        remaining = sorted(seq + items, key=lambda x: x[0])
        self.assertEqual(remaining, walk_items(tree))
        for x in seq:
            tree.remove_all(x[0])
            remaining.remove(x)
            self.assertEqual(remaining, walk_items(tree))
            self.assertEqual(len(remaining), tree.tree_size())

    def test_count(self):
        """Tests the count() function of a multitree"""
        tree = MultiBTree.create("tree", 3)
        self.assertEqual(tree.count(0), 0)
        for x in range(10):
            tree.insert(5, "test")
        self.assertEqual(tree.count(10), 0)
        self.assertEqual(tree.count(0), 0)
        self.assertEqual(tree.count(5), 10)
        for x in range(10):
            tree.insert(x, str(x))
        for x in range(10):
            if x == 5:
                self.assertEqual(tree.count(x), 11)
            else:
                self.assertEqual(tree.count(x), 1)

    def test_contains(self):
        """Tests the count() function of a multitree"""
        tree = BTree.create("tree", 3)
        for x in range(10):
            tree.insert(x, str(x))
        for x in range(10):
            self.assertEqual(x in tree, True)
        self.assertEqual(11 not in tree, True)

        tree = MultiBTree.create("tree", 3)
        tree.update((5, "test") for x in range(10))
        self.assertEqual(5 in tree, True)
        self.assertEqual(5 not in tree, False)
        self.assertEqual(6 in tree, False)

    def test_identifiers(self):
        tree = MultiBTree2.create("tree", 3)
        seq = list(range(20))
        tree.update((x, str(x), str(x)) for x in seq)
        self.assertEqual(seq, walk_keys(tree))
        seq2 = list(range(20, 40))
        for s2, s1 in zip(seq2, seq):
            tree.insert(s2 , str(s2), str(s1))
        self.assertEqual(seq2, walk_keys(tree))
        self.validate_indices(tree)
        self.assertRaises(ValueError, tree.insert, 123, 'abc', None)
        self.assertRaises(ValueError, tree.insert, 123, 'abc', 123)
        with self.assertRaises(ValueError):
            tree.update([(123, 'abc', 'abc'), (234, 'def', None)])

    def test_identifiers_in_transaction(self):
        """
        Tests with identical identifiers in a transaction.
        """
        tree = MultiBTree2.create("tree", 3)
        tree.insert(0, "v", "50")
        # Test inserting multiple identifiers in a tree
        # when a item with that identifier already exists
        def txn():
            tree.insert(100, "v", "50")
            tree.insert(0, "v2", "50")
        ndb.transaction(txn)
        self.assertEqual(0 in tree, True)
        self.assertEqual(100 in tree, False)
        # Test multiple inserts with the same identifier in a
        # transaction, when that identifier is not yet in
        # the tree...
        def txn():
            tree.insert(10, "test", "testing")
            tree.insert(11, "test", "testing")
            tree.insert(12, "test", "testing")
        ndb.transaction(txn)
        self.assertEqual(10 not in tree, True)
        self.assertEqual(11 not in tree, True)
        self.assertEqual(12 in tree, True)
        def txn():
            tree.insert(15, "test", "testing")
            tree.remove_all(15)
            tree.insert(20, "test", "testing")
        ndb.transaction(txn)
        self.assertEqual(20 in tree, True)
        self.assertEqual(15 not in tree, True)
        self.validate_indices(tree)


    def test_get_remove_identifiers(self):
        """
        Tests the get and remove by identifier functions
        """
        tree = MultiBTree2.create("tree", 3)

        tree.update((1, str(x), str(x)) for x in range(30))
        for x in range(30):
            self.assertEqual((1, str(x), str(x)),
                             tree.get_by_identifier(str(x)))
        for x in range(30):
            tree.remove_by_identifier(str(x))
            self.assertEqual(None, tree.get_by_identifier(str(x)))
        self.assertEqual(tree.tree_size(), 0)
        item = (10, "10", "id")
        item2 = (11, "11", "id2")
        def txn():
            tree.update([item, item2])
            self.assertEqual(10 in tree, True)
            self.assertEqual(11 in tree, True)
            self.assertEqual(tree.get_by_identifier("id"), item)
            self.assertEqual(tree.get_by_identifier("id2"), item2)
            tree.remove_by_identifier("id")
            self.assertEqual(tree.get_by_identifier("id"), None)
        ndb.transaction(txn)
        self.assertEqual(tree.tree_size(), 1)
        self.assertEqual(tree.get_by_identifier("id"), None)
        self.assertEqual(tree.get_by_identifier("id2"), item2)
        self.validate_indices(tree)


    def test_index_operations(self):
        """
        Test the various index() functions.
        """
        tree = BTree.create("tree", 3)
        tree.update((x, str(x)) for x in range(50))
        for x in range(50):
            self.assertEqual(x, tree.index(x))
        self.assertRaises(ValueError, tree.index, -10)

        tree = MultiBTree.create("tree", 3)
        tree.update((x, str(x)) for x in range(50))
        for x in range(50):
            self.assertEqual(x, tree.index(x))
            self.assertEqual(x + 1, tree.index_right(x))
        self.assertRaises(ValueError, tree.index_left, -1)
        self.assertRaises(ValueError, tree.index_right, -1)
        tree = MultiBTree2.create("tree", 3)
        tree.update((x % 5, str(x), str(x)) for x in range(50))
        for x in range(5):
            self.assertEqual(tree.count(x), 10)
            self.assertEqual(tree.index_left(x), 10 * x)
            self.assertEqual(tree.index_right(x), 10 * x + 10)
        self.assertRaises(ValueError, tree.index, 5)
        self.assertRaises(ValueError, tree.index_right, 5)


    def test_batch_operations(self):
        """
        Tests batch() calls.
        """
        tree = BTree.create("tree", 3)
        def f():
            for x in range(50):
                tree.insert(x, str(x))
        tree.perform_in_batch(f)

        def f():
            for x in range(50):
                tree.remove(x)
        tree.perform_in_batch(f)
        self.assertEqual([], tree[:])


        tree = BTree.create("tree", 3)
        def f():
            for x in range(50):
                tree.insert(x, str(x))
            for x in range(50):
                tree.remove(x)
        tree.perform_in_batch(f)
        self.assertEqual([], tree[:])


    def test_types(self):
        """
        Test various other types for the trees.
        """
        tree = BTree.create("tree", 3)
        items = [(str(x), x) for x in range(30)]
        items.sort()
        tree.update(items)
        self.assertEqual(items, tree[:])

    def test_updates_with_identifiers(self):
        """
        Tests the replacement of multiple items by identifier, in a
        single update operation.
        """
        tree = MultiBTree2.create("tree", 3)
        items = [("abc", x, str(x)) for x in range(50)]
        # Test duplicate inserts, also tests identiifier '0'.
        tree.update(items)
        tree.update(items)
        self.assertEqual(items, tree[:])
        items2 = [("def", x, str(x)) for x in range(50)]
        tree.update(items2)
        self.assertEqual(items2, tree[:])
        self.validate_indices(tree)


    def test_pop_results(self):
        """
        Tests the return values of the pop() operation. Also tests
        deletion on medians.
        """
        tree = BTree.create("tree", 5)
        items = [(str(x), x) for x in range(50)]
        items.sort()
        tree.update(items)
        while items:
            # Delete medians. This produced an error once.
            idx = len(items) / 2
            seq_item = items.pop(idx)
            tree_item = tree.pop(idx)
            self.assertEqual(seq_item, tree_item)
        self.validate_empty_tree(tree)


    def test_lower_upper_bounds(self):
        """
        Tests the lower/upper bound functions.
        """
        tree = BTree.create("tree", 3)
        tree.update((2 * x, str(2 * x)) for x in range(1, 50))
        for i, x in enumerate(range(1, 100, 2)):
            index_lower = tree.lower_bound(x)
            self.assertEqual(index_lower, i)

        tree = MultiBTree.create("tree", 3)
        tree.update((2 * x, str(2 * x)) for x in range(1, 50))
        for i, x in enumerate(range(1, 100, 2)):
            index_lower = tree.lower_bound(x)
            index_upper = tree.upper_bound(x)
            self.assertEqual(index_lower, i)
            self.assertEqual(index_lower, index_upper)

        tree = MultiBTree.create("tree", 3)
        values = ([1, 1, 1, 2, 2, 3, 3, 3, 3, 4, 5, 5, 5, 6, 7, 8, 8, 8,
                   9, 9, 9, 9, 10, 11, 12, 13, 15, 15, 15, 15, 16, 16, 16,
                   17, 17, 17, 18, 18, 19, 19, 19, 19, 19, 19, 19, 19, 19])
        tree.update((value, value) for value in values)
        import bisect
        for i in range(20):
            self.assertEqual(bisect.bisect_left(values, i),
                             tree.lower_bound(i))
            self.assertEqual(bisect.bisect_right(values, i),
                             tree.upper_bound(i))


    def test_missing_btree_index(self):
        """
        Test to reproduce the bug where an item with an identifier is
        still in the tree, but the corresponding _BTrreeIndex is not
        there.
        """
        tree = MultiBTree2.create("tree", 3)
        items = [("abc+%s" % x, x, str(x)) for x in range(50)]
        tree.update(items)
        # Delete half of the items, then insert them again using
        # the same pattern that created the bug.
        for x in range(0, 50, 2):
            tree.remove_by_identifier(str(x))
            def f():
                item = tree.get_by_identifier(str(x))
                tree.insert("qqq%s" % x, x * 10, str(x))
            tree.perform_in_batch(f)

        self.validate_indices(tree)


    def test_left_right_delete_by_index(self):
        """
        Tests a delete where the deleted key is to be swapped
        by the last item in the left child node.
        """
        tree = MultiBTree2.create("tree", 5)
        # Build a full tree
        for x in reversed(range(25)):
            tree.insert(x, "o", str(x))
        for x in reversed(range(25)):
            tree.insert(x, "o", str(50 + x))
        self.validate_tree(tree)
        tree.remove_by_identifier('15')
        self.validate_tree(tree)
        # right delete
        tree.remove_by_identifier('5')
        self.validate_tree(tree)


    def test_delete_child_minimum_degree(self):
        """
        Tests all cases of an internal function
        """
        tree = MultiBTree2.create("tree", 5)
        # Build a full tree
        for x in reversed(range(25)):
            tree.insert(x, "o", str(x))
        for x in reversed(range(25)):
            tree.insert(x, "o", str(50 + x))
        # Essentially there are 4 cases:
        # 1) get an item fram the left sibling
        tree.remove_by_identifier('3')
        # 2) get an item from the right sibling
        tree.remove_by_identifier('53')
        self.validate_tree(tree)
        # 3) merge with the right sibling
        tree.remove_by_identifier('1')
        self.validate_tree(tree)
        # 4) merge with the left sibling
        tree.remove_by_identifier('14')
        tree.remove_by_identifier('64')
        tree.remove_by_identifier('13')
        tree.remove_by_identifier('63')
        tree.remove_by_identifier('60')
        tree.remove_by_identifier('9')
        tree.remove_by_identifier('59')
        tree.remove_by_identifier('4')
        tree.remove_by_identifier('54')
        tree.remove_by_identifier('0')
        tree.remove_by_identifier('6')
        tree.remove_by_identifier('2')
        tree.remove_by_identifier('7')
        tree.remove_by_identifier('8')
        self.validate_tree(tree)
        # Delete remaining
        items = tree[:]
        def f():
            for item in items:
                tree.remove_by_identifier(item[2])
        tree.perform_in_batch(f)
        self.validate_empty_tree(tree)

    def test_print_tree(self):
        """
        Tests the print tree functions. These are for debugging
        only.
        """
        tree = MultiBTree2.create("tree", 5)
        tree._print_tree()
        tree._print_tree_summary()
        for x in range(100):
            tree.insert(x, "o", str(x))
        tree._print_tree()
        tree._print_tree_summary()



def main():
    fast = unittest.TestSuite()
    fast.addTest(BTreeTest('test_get_or_create'))
    unittest.TextTestRunner().run(fast)
    unittest.main()


if __name__ == '__main__':
    main()
