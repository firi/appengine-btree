"""
Actual BTree implementation. Do not use directly.
"""
__author__ = "Tijmen Roberti"
__license__ = "Apache 2.0"

import bisect
from itertools import izip, izip_longest, chain
from google.appengine.ext import ndb


class _BTreeNode(ndb.Model):
    """
    _BTreeNodes store the actual key/value pairs and links to the
    other nodes; they form the actual tree.
    """
    # Memcache is disabled, as the nodes are almost always retrieved
    # in a transaction, so memcache is not used anyway.
    _use_memcache = False
    # All lists are sorted, and the arrays are 'parallel', so each
    # item at the i'th index belong together.
    #
    # The maximum size of the lists is determined by degree of the
    # tree.
    #
    # Keys can be any comparable object. Pickled properties are used
    # because of their fast serialization and deserialization, as the
    # properties are not indexed anyway.
    keys = ndb.PickleProperty('k', indexed=False)
    values = ndb.PickleProperty('v', indexed=False)
    # Optional ids. If no ids are used, this array is empty, as it
    # saves datastore writes that way. The ids are separately indexed
    # in another entity, which refers back to the corresponding key,
    # value pair, so the entry in the tree can be found again.
    ids = ndb.PickleProperty('i', indexed=False)
    # Links to other child nodes. The number of links is len(keys) +
    # 1, as the keys act as a separator.
    links = ndb.PickleProperty('l', indexed=False)
    # The sizes of the subtree for each corresponding link.
    counts = ndb.PickleProperty('c', indexed=False)
    # The original identifier of this node when it was created. When a
    # node becomes a root node, it takes as id "root" instead of its
    # assigned id. If another node becomes the root node, it reverts
    # back to its original assigned id, that is stored in this
    # property.
    assigned_id = ndb.IntegerProperty('aid', indexed=False)


    def is_leaf(self):
        return not bool(self.links)

    def size(self):
        return len(self.keys)

    def insert(self, index, item):
        """
        Inserts the (key, value, id) item at the given index in this
        node.
        """
        self.keys.insert(index, item[0])
        self.values.insert(index, item[1])
        if item[2] is not None:
            self.ids.insert(index, item[2])
            self._parent_tree._identifier_added(item[2], item[0], item[1])

    def append(self, item):
        """
        Appends the (key, value, id) item at the end of the entries in
        this node.
        """
        self.keys.append(item[0])
        self.values.append(item[1])
        if item[2] is not None:
            self.ids.append(item[2])
            self._parent_tree._identifier_added(item[2], item[0], item[1])

    def replace(self, index, item):
        """
        Replaces the existing values at the given |index| with the
        values of the new (key, value, id) item.
        """
        self.keys[index] = item[0]
        self.values[index] = item[1]
        if item[2] is not None:
            old_id = self.ids[index]
            self.ids[index] = item[2]
            self._parent_tree._identifier_removed(old_id)
            self._parent_tree._identifier_added(item[2], item[0], item[1])

    def item(self, index):
        """
        Returns the (key, value, id) tuple at the given |index|.
        """
        return (self.keys[index], self.values[index],
                self.ids[index] if self.ids else None)

    def pop_item(self, index=-1):
        """
        Pops the (key, value, id) tuple at the given |index|, or the last
        item if no index is provided.
        """
        popped = (self.keys.pop(index), self.values.pop(index),
                  self.ids.pop(index) if self.ids else None)
        if popped[2] is not None:
            self._parent_tree._identifier_removed(popped[2])
        return popped

    def tree_size(self):
        """
        Returns the size of the tree formed by this node.
        """
        return sum(self.counts) + len(self.keys)

    def extend_with_contents_of_node(self, node):
        """
        Extend this node's key, values and ids with the contents of
        those lists in the given |node|.
        """
        self.keys.extend(node.keys)
        self.values.extend(node.values)
        if node.ids:
            self.ids.extend(node.ids)
            # No need to callback the identifiers, as this is a move
            # only.
        self.links.extend(node.links)
        self.counts.extend(node.counts)

    def iteritems(self, start=None, end=None):
        """
        Returns an iterator that yields all items as (key, value)
        pairs if no ids are specified, otherwise, yields (key, value,
        id) pairs. |start| and |end| can be used to specify a slice as
        normal.
        """
        if self.ids:
            return izip_longest(self.keys[start:end],
                                self.values[start:end],
                                self.ids[start:end])
        else:
            return izip_longest(self.keys[start:end], self.values[start:end])

    def items(self, start, end):
        """
        Returns a list of items from this node in the range
        [start:end).  The list contains (key, value) pairs if no ids
        are present, otherwise it contains (key, value, identifier
        pairs).
        """
        if self.ids:
            return zip(self.keys[start:end],
                       self.values[start:end],
                       self.ids[start:end])
        else:
            return zip(self.keys[start:end], self.values[start:end])


    def __str__(self):
        return ("Node(id=%s, keys=%s, values=%s, ids=%s, links=%s, counts=%s)"
                % (self.key.id(), self.keys, self.values,
                   self.ids, self.links, self.counts))


class _BTreeIndex(ndb.Model):
    """
    An index node, which uses the identifier as keyname, and stores
    the associated (key, value) pair so it can be quickly retrieved,
    but with the key the entry can also be found relative quickly.
    """
    _use_memcache = False
    # The key, value pair associated with this identifier that is
    # Stored in the tree.
    tree_key = ndb.PickleProperty('k', indexed=False)
    tree_value = ndb.PickleProperty('v', indexed=False)


class _BTreeBase(ndb.Model):
    """
    The tree base class. The tree only contains a single member variable,
    the degree of the tree. This degree never changes, and as such the tree
    itself never changes and can safely be cached.

    The root node key is not explicitly stored. Instead, the root node
    always has the entity identifier "root", and as parent key this
    tree's key. This prevents potential transaction issues where a
    tree is retrieved outside a transaction and also prevents any
    caching issues.
    """
    # Minimum degree of the tree, set once during creation. Never
    # changes.
    degree = ndb.IntegerProperty(indexed=False, required=True)


    def _initialize(self, minimum_degree):
        """
        Initializes this instance. Creates a root node and sets
        the degree of the tree.
        """
        if minimum_degree < 2:
            raise ValueError("Minimum degree of tree must be 2 or greater")
        if not self.key:
            raise ValueError("Cannot initialize a tree without a key")
        root = self._make_node()
        root.key = self._make_node_key("root")
        self.degree = minimum_degree
        ndb.put_multi([root, self])
        return self


    def _batch_operations(self, func):
        """
        Setups a memory cache and a transaction to execute the
        operation specified in the function |func|. Performing
        multiple operations on the tree in |func| allows for optimal
        caching of nodes. _batch_operations() can also be nested, with
        the nested calls having no effect on the cache.

        If |func| is finished, all changes to the tree will be flushed
        to the datastore.

        All operations must be part of a call to _batch_operations, as
        it sets up caches that are used in most calls.
        """
        def txn():
            first_batch_call = not all([hasattr(self, "_nodes_to_put"),
                                        hasattr(self, "_indices_to_put"),
                                        hasattr(self, "_identifier_cache"),
                                        hasattr(self, "_keys_to_delete")])
            if first_batch_call:
                self._nodes_to_put = dict()
                self._indices_to_put = dict()
                self._identifier_cache = dict()
                self._keys_to_delete = set()
            try:
                results = func()
                if first_batch_call and any([self._nodes_to_put,
                                             self._indices_to_put,
                                             self._keys_to_delete]):
                    futures = ndb.delete_multi_async(self._keys_to_delete)
                    ndb.put_multi(chain(self._nodes_to_put.itervalues(),
                                        self._indices_to_put.itervalues()))
                    [future.get_result() for future in futures]
            finally:
                if first_batch_call:
                    del self._nodes_to_put
                    del self._indices_to_put
                    del self._identifier_cache
                    del self._keys_to_delete
            return results

        if ndb.in_transaction():
            return txn()
        else:
            return ndb.transaction(txn)


    def _put_node(self, *args):
        """
        Queues all nodes in *args to be put() when all operations are
        completed for the current batch. This function can only be
        used in a function that is called as part of a call of
        _batch_operations().
        """
        # If we put a node after it is queued for deletion, then
        # remove it from the to be deleted nodes, as it will be
        # overwritten automatically. This is also required, to avoid
        # the situation where a node with the same key would be
        # deleted and overwritten in the same transaction.
        for node in args:
            self._keys_to_delete.discard(node.key)
        self._nodes_to_put.update((node.key, node) for node in args)


    def _delete_node(self, *args):
        """
        Queues all nodes in args be deleted when all operations are
        completed. This function can only be used in function that is
        called as part of call of _batch_operations().
        """
        for node in args:
            if node.key in self._nodes_to_put:
                del self._nodes_to_put[node.key]
            # Need to copy the key, as a node can change its key if it
            # becomes the root node.
            self._keys_to_delete.add(node.key)


    def _get_by_key(self, item_key):
        """
        Returns the first item that matches the given |item_key|. If
        the key does not exist, returns None.
        """
        def in_order(node):
            if node.size() == 0:
                return None
            i = bisect.bisect_left(node.keys, item_key)
            # Subtree items are always 'first'
            item = None
            if not node.is_leaf():
                item = in_order(self._get_node(node.links[i]))
            # If nothing is found in subtree, try if the item is in
            # this node.
            if item is None and i < node.size() and node.keys[i] == item_key:
                item = node.items(i, i + 1)[0]
            return item

        return in_order(self._get_root())


    def _get_all_by_key(self, item_key):
        """
        Returns a list of items that match the given |key|.
        """
        def in_order(node):
            i = bisect.bisect_left(node.keys, item_key)
            j = bisect.bisect_right(node.keys, item_key)
            if node.is_leaf():
                return node.items(i, j)

            results = []
            for link, item in izip_longest(node.links[i:j+1],
                                           node.iteritems(i, j)):
                results.extend(in_order(self._get_node(link)))
                if item is not None:
                    results.append(item)
            return results
        return in_order(self._get_root())


    def _get_by_identifier(self, identifier):
        """
        Returns the item with the given identifier, or None if no such
        item exists.
        """
        assert isinstance(identifier, basestring), "Identifiers must be strings"
        key_and_value = self._key_and_value_for_identifier(identifier)
        if key_and_value is not None:
            return key_and_value + (identifier,)
        return None


    def _get_by_index(self, index):
        """Returns the item at the given index."""
        if index < 0:
            index = self._size() + index
        items = self._get_by_index_range(index, 1)
        return items[0]


    def _get_by_index_range(self, start_index=0, num=0):
        """
        Returns a list of items in the range [start, start + num). If
        identifiers are used, the items returned are tuples of size 3,
        otherwise only the (key, value) pair is returned.
        """
        if start_index < 0:
            raise IndexError("Start index %s cannot be negative" % start_index)

        def in_order(node, index, n):
            assert index >= 0, "Index cannot be negative"
            if node.is_leaf():
                return node.items(index, index + n)
            # Internal node
            results = []
            for link, count, item in izip_longest(node.links,
                                                  node.counts,
                                                  node.iteritems()):
                if index - count  >= 0: # skip subtrees
                    if index - count == 0 and n > 0:
                        results.append(item)
                        n -= 1
                    index -= count + 1
                    continue
                items = in_order(self._get_node(link), max(index, 0), n)
                results.extend(items)
                n -= len(items)
                if item is not None and n > 0:
                    results.append(item)
                    n -= 1
                index -= count + 1
                if n == 0:      # stop directly to prevent excess get_nodes()
                    break
            return results

        return in_order(self._get_root(), start_index, num)


    def _insert(self, key, value, identifier, allow_duplicates=False):
        if identifier is not None:
            if  not isinstance(identifier, basestring):
                raise ValueError("Identifiers must be strings")
            self._delete_identifier(identifier)

        root = self._get_root()
        if self._is_full(root):
            # Grow the tree by one, creating a new root.
            new_root = self._make_node()
            new_root.key = self._make_node_key("root")
            # Revert the old root's key back to its original id. This
            # requires careful swapping, and the nodes are also
            # immediately put, to update the in memory cache for the
            # new keys.
            root.key = self._make_node_key(root.assigned_id)
            new_root.links.insert(0, root.key.id())
            new_root.counts.insert(0, root.tree_size())
            self._put_node(root, new_root)
            root = new_root
            self._split_child_node(new_root, 0)

        self._do_insert(root, key, value, identifier,
                        duplicate_keys=allow_duplicates)


    def _delete_key(self, key):
        """
        Delete a single item with the given |key|. Do not use for
        multitrees.  Returns the deleted item, or None if no item with
        that key exists.
        """
        root = self._get_root()
        item = self._do_delete(root, key)
        self._replace_root_if_required(root)
        if item is not None:
            return item if item[2] is not None else item[:2]
        else:
            return None

    def _delete_key_all(self, key):
        """
        Deletes all entries with the given |key|. Used by
        multitrees. Returns a list of deleted items.
        """
        i = self._left_index_of_key(key)
        j = self._right_index_of_key(key)
        if i < 0 or j < 0:
            return []
        else:
            return self._delete_range(i, j)

    def _delete_index(self, index):
        """
        Deletes the entry at the given index. Returns the deleted
        item.
        """
        root = self._get_root()
        item = self._do_delete_by_index(root, index)
        self._replace_root_if_required(root)
        return item if item[2] is not None else item[:2]

    def _delete_range(self, a, b):
        """
        Deletes all items formed by the range [a, b). Returns a list
        of the deleted items.
        """
        assert a >= 0 and b >= 0, "Cannot delete negative range"

        root = self._get_root()
        deleted = []
        for x in xrange(a, b):
            item = self._do_delete_by_index(root, a)
            root = self._replace_root_if_required(root)
            deleted.append(item if item[2] is not None else item[:2])
        return deleted


    def _delete_identifier(self, identifier):
        """
        Removes the item with the given |identifier|. If no item with
        that identifier exist, then this function does not perform any
        operations.
        """
        key_and_value = self._key_and_value_for_identifier(identifier)
        if key_and_value is not None:
            item_index = self._index_for_key_and_identifier(key_and_value[0],
                                                            identifier)
            assert item_index != -1, ("Item '%s' missing! Key:'%s'. Tree:%s" %
                                      (identifier, key_and_value[0], self.key))
            self._delete_index(item_index)


    def _replace_root_if_required(self, root):
        """
        Sets a new root of this tree, if the given |root| is empty and
        has a single child node. Returns an instance of the new root
        node, or the old root if it is not empty.
        """
        if root.size() == 0 and len(root.links) > 0:
            assert len(root.links) == 1, "Cannot have more than one child"
            new_root = self._get_node(root.links[0])
            # Delete the entity with the old numeric key, and save the
            # entity again, but this time using the root key.
            self._delete_node(new_root)
            new_root.key = root.key
            self._put_node(new_root) # overwrites old root
            root = new_root
        return root


    def _split_child_node(self, node, i):
        """
        Splits the |i|'th child node of |node| in two, adding them as
        child nodes of |node|. The median key of the child node is
        moved to the |node|. |node| cannot be a full node.
        """
        assert not self._is_full(node), "Node is full"
        split = self._get_node(node.links[i])
        assert self._is_full(split), "Child node %s is not full" % split
        new = self._make_node()
        # The median key of the split node will be the separator for
        # the new nodes. The separator and associated values will go
        # in the parent node.
        n = self.degree - 1               # separator index
        node.insert(i, split.item(n))
        # Split the existing values, the values beyond the separator
        # go into the new node, while the lower values stay in the split
        # node.
        split.keys, new.keys = split.keys[:n], split.keys[n+1:]
        split.values, new.values = split.values[:n], split.values[n+1:]
        split.ids, new.ids = split.ids[:n], split.ids[n+1:]
        split.links, new.links = split.links[:n+1], split.links[n+1:]
        split.counts, new.counts = split.counts[:n+1], split.counts[n+1:]
        # Update parent links. The original link to the split node is
        # already in the correct position.
        node.links.insert(i + 1, new.key.id())
        node.counts[i] = split.tree_size()
        node.counts.insert(i + 1, new.tree_size())
        self._put_node(node, new, split)


    def _do_insert(self, node, key, value, identifier, duplicate_keys=True):
        """
        Recursively insert the key, value and id in the non-full
        |node|. This procedure will split nodes along the way if
        required. Duplicate keys are allowed.

        Returns the size of the tree.
        """
        i = bisect.bisect(node.keys, key)

        if (not duplicate_keys
            and 0 <= (i - 1) < node.size()
            and node.keys[i - 1] == key):
            node.replace(i - 1, (key, value, identifier))
            self._put_node(node)
            return node.tree_size()

        if node.is_leaf():
            node.insert(i, (key, value, identifier))
        else:
            child = self._get_node(node.links[i])
            if self._is_full(child):
                self._split_child_node(node, i)
                # The current node has changed due to the split, and
                # the key might have moved up in the node. Just retry
                # the insert.
                #
                # TODO(tijmen): This 'trick' does require removing the
                # not full assertion at the start of the function, as
                # the current node could have become full due to the
                # splitting. This is not very elegant, but it saves
                # duplicating logic.
                return self._do_insert(node, key, value, identifier,
                                       duplicate_keys)
            child_size = self._do_insert(child, key, value, identifier,
                                         duplicate_keys)
            node.counts[i] = child_size
        # Must always save as the tree size will have changed or
        # an item is inserted.
        self._put_node(node)
        return node.tree_size()


    def _do_delete_by_index(self, node, index):
        """
        Deletes the |index| entry in the tree formed by node.

        Raises:
           IndexError if the index is out of range.
        """
        if node.is_leaf():
            deleted_item = node.pop_item(index)
            self._put_node(node)
            return deleted_item

        # Find subtree for the item with the absolute |index|, if it
        # is in a subtree. If not in a subtree, the item lies in this
        # node and |i| contains the index to that item.
        index_in_subtree = False
        for i, count in enumerate(node.counts):
            # Skip subtrees until the index is reached
            if index - count <= 0:
                index_in_subtree = (index - count) != 0
                break
            index -= count + 1

        if index_in_subtree:
            child, child_i, offset = self._child_with_minimum_degree(node, i)
            deleted_item = self._do_delete_by_index(child, index + offset)
            node.counts[child_i] = child.tree_size()
            self._put_node(node)
            return deleted_item

        # |index| points to a key in this node. |i| contains the local
        # index of the item in this node.
        #
        # If the child nodes are large enough, replace the key by a
        # predecessor or successor in one of the subtrees. This
        # maintains the tree and completes the deletion.
        left = self._get_node(node.links[i])
        if left.size() >= self.degree:
            deleted_item = self._do_delete_by_index(left, index - 1)
            item = node.item(i)
            node.replace(i, deleted_item)
            node.counts[i] = left.tree_size()
            self._put_node(node)
            return item

        right = self._get_node(node.links[i + 1])
        if right.size() >= self.degree:
            deleted_item = self._do_delete_by_index(right, 0)
            item = node.item(i)
            node.replace(i, deleted_item)
            node.counts[i + 1] = right.tree_size()
            self._put_node(node)
            return item

        # Both children do not have enough keys, so merge both
        # children. This will put the item to be deleted in the child
        # node.
        child = self._merge_with_right_sibling(node, i)
        median = child.size() / 2
        new_index = median + sum(child.counts[:median + 1])
        deleted_item = self._do_delete_by_index(child, new_index)
        node.counts[i] -= 1
        self._put_node(node)
        return deleted_item


    def _do_delete(self, node, key):
        """
        Deletes the left most entry of |key| in |node|. Returns the
        deleted (key, value, id) pair, or None if the key does not
        exist in the tree.

        This function is slightly faster cpu wise than deletion by
        key, as it uses binary search to traverse the tree.

        NOTE:
        This function should not be used to delete items in a
        multitree. Instead, deleting an item by index should be
        used for those trees.
        """
        i = bisect.bisect_left(node.keys, key)

        contains_key = (i < len(node.keys) and node.keys[i] == key)
        if node.is_leaf():
            if not contains_key:
                return None     # key not in the tree
            deleted_item = node.pop_item(i)
            self._put_node(node)
            return deleted_item
        #
        # node is an internal node
        #
        if not contains_key:
            child, index, _ = self._child_with_minimum_degree(node, i)
            deleted_item = self._do_delete(child, key)
            node.counts[index] = child.tree_size()
            self._put_node(node)
            return deleted_item

        # node is an internal node, and |key| is in this node.
        #
        # If the child nodes are large enough, replace the item by a
        # predecessor or successor in one of the subtrees. This
        # maintains the tree and completes the deletion.
        left = self._get_node(node.links[i])
        if left.size() >= self.degree:
            p_key = self._find_predecessor(left, key)
            deleted_item = self._do_delete(left, p_key)
            assert deleted_item and p_key == deleted_item[0]
            item = node.item(i)
            node.replace(i, deleted_item)
            node.counts[i] = left.tree_size()
            self._put_node(node)
            return item

        right = self._get_node(node.links[i + 1])
        if right.size() >= self.degree:
            s_key = self._find_successor(right, key)
            deleted_item = self._do_delete(right, s_key)
            assert deleted_item and s_key == deleted_item[0]
            item = node.item(i)
            node.replace(i, deleted_item)
            node.counts[i + 1] = right.tree_size()
            self._put_node(node)
            return item

        # Both children do not have enough keys, so merge both
        # children. This will put the |key| in the child node.
        child = self._merge_with_right_sibling(node, i)
        deleted_item = self._do_delete(child, key)
        node.counts[i] -= 1
        self._put_node(node)
        return deleted_item


    def _child_with_minimum_degree(self, node, index):
        """
        Returns the child of |node| at the given index, after
        augmenting the child such that it has at least the
        minimum_degree number of keys, by either moving a key
        from its immediate siblings, or merging with one of them.

        Returns a tuple (child, index, offset), where child is
        requested child instance, index the new index of the child
        node, as it might have been changed due to a merge
        operation. Finally, the offset returned is the relative shift
        in keys due to the various operation, and is only used when
        deleting items by index.
        """
        child = self._get_node(node.links[index])
        if child.size() >= self.degree:
            return child, index, 0

        if index > 0:
            left = self._get_node(node.links[index - 1])
            if left.size() >= self.degree:
                # Move a key from the left sibling to the child. The
                # item is first deleted (with replace) and then added
                # again, to ensure that the identifier index stays
                # correct.
                item = node.item(index - 1)
                node.replace(index - 1, left.pop_item())
                child.insert(0, item)
                if not left.is_leaf():
                    child.links.insert(0, left.links.pop())
                    child.counts.insert(0, left.counts.pop())
                node.counts[index - 1] = left.tree_size()
                node.counts[index] = child.tree_size()
                self._put_node(node, child, left)
                return child, index, 1
        else:
            left = None

        if index < node.size():
            right = self._get_node(node.links[index + 1])
            if right.size() >= self.degree:
                # Move a key from the right sibling to the child. The
                # item is first deleted (with replace) and then added
                # again, to ensure that the identifier index stays
                # correct.
                item = node.item(index)
                node.replace(index, right.pop_item(0))
                child.append(item)
                if not right.is_leaf():
                    child.links.append(right.links.pop(0))
                    child.counts.append(right.counts.pop(0))
                node.counts[index] = child.tree_size()
                node.counts[index + 1] = right.tree_size()
                self._put_node(node, child, right)
                return child, index, 0
        else:
            right = None

        # No immediate siblings, or not enough keys. Merge
        # one of them in the child node.
        if left:
            # Take the offset before the merge operation, as that
            # increases the size. Also, add one for the key from
            # |node| that moved downwards to the child.
            offset = left.size() # take offset before merge operation
            return (self._merge_with_right_sibling(node, index - 1),
                    index - 1,
                    offset + 1)
        elif right:
            return (self._merge_with_right_sibling(node, index), index, 0)
        else: # pragma: no cover
            assert False, "Must have a sibling to merge with"


    def _merge_with_right_sibling(self, node, index):
        """
        Merges the child node at |index| with its immediate right
        sibling. Both the child and right node must have t - 1 keys
        each. The key at index of |node| will be moved downwards to
        the child. The right sibling will be deleted.

        Returns the instance of the child node
        """
        assert index < len(node.keys), "No right sibling"
        left = self._get_node(node.links[index])
        right = self._get_node(node.links[index + 1])
        assert left.size() == self.degree - 1, "Too large left sibling"
        assert right.size() == self.degree - 1, "Too large right sibling"

        left.append(node.pop_item(index))
        left.extend_with_contents_of_node(right)
        node.links.pop(index + 1) # remove link and free right node
        node.counts.pop(index + 1)
        node.counts[index] = left.tree_size()
        self._put_node(node, left)
        self._delete_node(right)
        return left


    def _lower_bound_index(self, key):
        """
        Returns the index to the first element whose key is not less than
        |key|.
        """
        def find_key(node):
            i = bisect.bisect_left(node.keys, key)
            if node.is_leaf():
                return i
            else:
                count = sum(node.counts[:i])
                return count + i + find_key(self._get_node(node.links[i]))
        return find_key(self._get_root())


    def _upper_bound_index(self, key):
        """
        Returns the index to the first element whose key is strictly
        greater than |key|.
        """
        def find_key(node):
            i = bisect.bisect_right(node.keys, key)
            if node.is_leaf():
                return i
            else:
                count = sum(node.counts[:i])
                return count + i + find_key(self._get_node(node.links[i]))
        return find_key(self._get_root())


    def _left_index_of_key(self, key):
        """
        Returns the leftmost index of the item with |key|. Returns
        -1 if the key is not in the tree.
        """
        def find_key(node):
            if node.size() == 0:
                return -1
            i = bisect.bisect_left(node.keys, key)
            if node.is_leaf():
                return i if i < len(node.keys) and node.keys[i] == key else -1
            count = sum(node.counts[:i])
            index = find_key(self._get_node(node.links[i]))
            if index != -1:
                return count + index + i
            if i < len(node.keys) and key == node.keys[i]:
                return count + node.counts[i] + i
            return -1

        return find_key(self._get_root())


    def _right_index_of_key(self, key):
        """
        Returns the index one past the index of the last item with the
        given |key|. Returns -1 if the key is not in the tree.
        """
        def find_key(node):
            if node.size() == 0:
                return -1
            i = bisect.bisect_right(node.keys, key)
            if node.is_leaf():
                return i if i > 0 and node.keys[i - 1] == key else -1
            count = sum(node.counts[:i])
            index = find_key(self._get_node(node.links[i]))
            if index != -1:
                return count + index + i
            if i > 0 and key == node.keys[i - 1]:
                return count + i
            return -1

        return find_key(self._get_root())


    def _index_for_key_and_identifier(self, key, id):
        """
        Returns the index of the entry which matches the given key and
        id. If no item could be found, returns - 1.
        """
        def find_key_and_id(node):
            i = bisect.bisect_left(node.keys, key)
            j = bisect.bisect_right(node.keys, key)
            for x in xrange(i, j):
                if node.ids[x] == id:
                    return (sum(node.counts[:x + 1]) if node.counts else 0) + x
            if node.links:
                for x in range(i, j + 1):
                    index = find_key_and_id(self._get_node(node.links[x]))
                    if index != -1:
                        return sum(node.counts[:x]) + index + x
            return -1

        return find_key_and_id(self._get_root())


    def _find_predecessor(self, node, key):
        """
        Returns the key that is the predecessor of |key| in the subtree
        formed by |node|.
        """
        i = bisect.bisect_right(node.keys, key)
        if node.is_leaf():
            return node.keys[i - 1]
        else:
            return self._find_predecessor(self._get_node(node.links[i]), key)


    def _find_successor(self, node, key):
        """
        Returns the key that is the successor of |key| in the subtree
        formed by |node|.
        """
        i = bisect.bisect_left(node.keys, key)
        if node.is_leaf():
            return node.keys[i]
        else:
            return self._find_successor(self._get_node(node.links[i]), key)


    def _is_full(self, node):
        return len(node.keys) == (2 * self.degree - 1)


    def _get_root(self):
        """
        Retrieves the node instance that is the root node of this tree.
        """
        return self._get_node("root")


    def _get_node(self, node_id):
        """
        Retrieves the node with the given |node_id| from the datastore.
        """
        return self._get_node_from_key(self._make_node_key(node_id))


    def _get_node_from_key(self, node_key):
        """
        Gets the node from the given full datastore |node_key|. As an
        additional side effect, this function will also set the
        _parent_tree attribute that is used for callbacks in the node.
        """
        # First check if it is a node that has been created but not put()
        # yet, so it is not yet in the ndb transaction cache.
        if node_key in self._nodes_to_put:
            return self._nodes_to_put[node_key]
        # Get the node from ndb transaction cache, or from the datastore
        # if it hasn't been seen yet.
        node = node_key.get()
        assert node, "No node found with key %s" % (node_key,)
        node._parent_tree = self # used for callbacks
        return node


    def _make_node(self):
        """
        Makes a new node with an auto assigned id. The node must be
        stored sometime later using _put_node().
        """
        node_id = self._get_assigned_id()
        node = _BTreeNode(id=node_id, parent=self.key)
        node.populate(keys=[], values=[], ids=[], links=[], counts=[],
                      assigned_id=node_id)
        node.assigned_id = node.key.integer_id()
        node._parent_tree = self
        return node


    @ndb.non_transactional
    def _get_assigned_id(self):
        """
        Generate a unique integer identifier for a node.
        """
        # Allocate ids is not possible within a transaction for some
        # reason, so the non_transactional decorator is used to step
        # outside the current transaction.
        return _BTreeNode.allocate_ids(1, parent=self.key)[0]


    def _make_node_key(self, node_id):
         return ndb.Key(_BTreeNode, node_id, parent=self.key)


    def _print_tree(self):
        """
        Returns a string representation of the tree. Each node on a
        single line. For debugging purposes only.
        """
        def f():
            stack = [(self._get_root(), 0)]
            lines = []
            while stack:
                node, indent = stack.pop()
                lines.append(" " * indent + str(node))
                stack.extend(reversed([(self._get_node(link), indent + 4)
                                       for link in node.links]))
            return '\n'.join(lines)
        return self._batch_operations(f)


    def _print_tree_summary(self):
        """
        Returns a string representation of the tree. For debugging only.
        """
        def f():
            stack = [(self._get_root(), 0)]
            lines = ["tree size: %s" % self._size(),
                     "degree: %s" % self.degree,
                     ""]
            while stack:
                node, indent = stack.pop()
                line = " " * indent
                line += "Node(id: %s, %s items)" % (node.key.id(), len(node.keys))
                lines.append(line)
                stack.extend(reversed([(self._get_node(link), indent + 4)
                                       for link in node.links]))
            return '\n'.join(lines)
        return self._batch_operations(f)


    def _populate_identifier_cache(self, identifiers):
        """
        Fetches all identifiers from the identifier index in as
        few RPCs as possible. This function should be used when
        inserting large of amounts of items with identifiers.

        |identifiers| must be an iterable that yields identifiers.
        """
        identifiers = list(identifiers)
        keys = (ndb.Key(_BTreeIndex, id, parent=self.key) for id
                in identifiers)
        indices = ndb.get_multi(keys)
        key_values = []
        for index in indices:
            if index is not None:
                key_values.append((index.tree_key, index.tree_value))
            else:
                key_values.append(None)
        self._identifier_cache.update(izip(identifiers, key_values))


    def _key_and_value_for_identifier(self, identifier):
        """
        Returns the (key, value) pair associated with the given
        identifier. If the identifier is not used, None will be
        returned.

        This operation will perform one datastore get to retrieve the
        (key, value) pair.
        """
        # An in-memory identifier cache is used to track the mutations
        # of identifiers during replace or delete operations. If an
        # identifier is in the cache, it is always more recent than
        # the value in the datastore. None can also be returned as
        # value from the cache, which means that an identifier was
        # deleted. If a identifier is not in the cache, the value is
        # retrieved from the datastore.
        try:
            return self._identifier_cache[identifier]
        except KeyError:
            index = _BTreeIndex.get_by_id(identifier, parent=self.key)
            key_value = None
            if index:
                key_value = (index.tree_key, index.tree_value)
            self._identifier_cache[identifier] = key_value
            return key_value


    def _identifier_removed(self, identifier):
        """
        Callback used to notify that an item with the given |identifier|
        was deleted from a node.
        """
        index = self._make_index(identifier)
        self._keys_to_delete.add(index.key)
        if index.key in self._indices_to_put:
            del self._indices_to_put[index.key]
        # Setting None implies that the identifier is deleted.
        self._identifier_cache[identifier] = None


    def _identifier_added(self, identifier, key, value):
        """
        Callback to notify that an item with the given |identifier| and
        |key_and_value| pair was added to a node
        """
        index = self._make_index(identifier, key, value)
        self._indices_to_put[index.key] = index
        self._keys_to_delete.discard(index.key)
        self._identifier_cache[identifier] = (key, value)


    def _make_index(self, identifier, key=None, value=None):
        """
        Creates a _BTreeIndex instance.
        """
        return _BTreeIndex(id=str(identifier), parent=self.key,
                           tree_key=key, tree_value=value)

    def _size(self):
        """Returns the size of the BTree."""
        return self._get_root().tree_size()
