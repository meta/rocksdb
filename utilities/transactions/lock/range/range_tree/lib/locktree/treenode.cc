/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
#ifndef OS_WIN
#ident "$Id$"
/*======
This file is part of PerconaFT.


Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved.

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License, version 2,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.

----------------------------------------

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License, version 3,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.

----------------------------------------

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
======= */

#ident \
    "Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved."

#include "treenode.h"

#include "../portability/toku_race_tools.h"

namespace toku {

// TODO: source location info might have to be pulled up one caller
// to be useful
void treenode::mutex_lock(void) { toku_mutex_lock(&m_mutex); }

void treenode::mutex_unlock(void) { toku_mutex_unlock(&m_mutex); }

void treenode::init(const comparator *cmp) {
  m_txnid = TXNID_NONE;
  m_is_root = false;
  m_is_empty = true;
  m_cmp = cmp;

  m_is_shared = false;
  m_owners = nullptr;

  // use an adaptive mutex at each node since we expect the time the
  // lock is held to be relatively short compared to a context switch.
  // indeed, this improves performance at high thread counts considerably.
  memset(&m_mutex, 0, sizeof(toku_mutex_t));
  toku_pthread_mutexattr_t attr;
  toku_mutexattr_init(&attr);
  toku_mutexattr_settype(&attr, TOKU_MUTEX_ADAPTIVE);
  toku_mutex_init(treenode_mutex_key, &m_mutex, &attr);
  toku_mutexattr_destroy(&attr);
  m_left_child.set(nullptr);
  m_right_child.set(nullptr);
}

void treenode::create_root(const comparator *cmp) {
  init(cmp);
  m_is_root = true;
}

void treenode::destroy_root(void) {
  invariant(is_root());
  invariant(is_empty());
  toku_mutex_destroy(&m_mutex);
  m_cmp = nullptr;
}

void treenode::set_range_and_txnid(const keyrange &range, TXNID txnid,
                                   bool is_shared) {
  // allocates a new copy of the range for this node
  m_range.create_copy(range);
  m_txnid = txnid;
  m_is_shared = is_shared;
  m_is_empty = false;
}

bool treenode::is_root(void) { return m_is_root; }

bool treenode::is_empty(void) { return m_is_empty; }

bool treenode::range_overlaps(const keyrange &range) {
  return m_range.overlaps(*m_cmp, range);
}

treenode *treenode::alloc(const comparator *cmp, const keyrange &range,
                          TXNID txnid, bool is_shared) {
  treenode *XCALLOC(node);
  node->init(cmp);
  node->set_range_and_txnid(range, txnid, is_shared);
  return node;
}

void treenode::swap_in_place(treenode *node1, treenode *node2) {
  keyrange tmp_range = node1->m_range;
  TXNID tmp_txnid = node1->m_txnid;
  node1->m_range = node2->m_range;
  node1->m_txnid = node2->m_txnid;
  node2->m_range = tmp_range;
  node2->m_txnid = tmp_txnid;

  bool tmp_is_shared = node1->m_is_shared;
  node1->m_is_shared = node2->m_is_shared;
  node2->m_is_shared = tmp_is_shared;

  auto tmp_m_owners = node1->m_owners;
  node1->m_owners = node2->m_owners;
  node2->m_owners = tmp_m_owners;
}

bool treenode::add_shared_owner(TXNID txnid) {
  assert(m_is_shared);
  if (txnid == m_txnid)
    return false;  // acquiring a lock on the same range by the same trx

  if (m_txnid != TXNID_SHARED) {
    m_owners = new TxnidVector;
    m_owners->insert(m_txnid);
    m_txnid = TXNID_SHARED;
  }
  return m_owners->insert(txnid).second;
}

void treenode::free(treenode *node) {
  // destroy the range, freeing any copied keys
  node->m_range.destroy();

  if (node->m_owners) {
    delete node->m_owners;
    node->m_owners = nullptr;  // need this?
  }

  // the root is simply marked as empty.
  if (node->is_root()) {
    // PORT toku_mutex_assert_locked(&node->m_mutex);
    node->m_is_empty = true;
  } else {
    // PORT toku_mutex_assert_unlocked(&node->m_mutex);
    toku_mutex_destroy(&node->m_mutex);
    toku_free(node);
  }
}

uint32_t treenode::get_depth_estimate(void) const {
  const uint32_t left_est = m_left_child.depth_est;
  const uint32_t right_est = m_right_child.depth_est;
  return (left_est > right_est ? left_est : right_est) + 1;
}

treenode *treenode::find_node_with_overlapping_child(
    const keyrange &range, const keyrange::comparison *cmp_hint) {
  // determine which child to look at based on a comparison. if we were
  // given a comparison hint, use that. otherwise, compare them now.
  keyrange::comparison c =
      cmp_hint ? *cmp_hint : range.compare(*m_cmp, m_range);

  treenode *child;
  if (c == keyrange::comparison::LESS_THAN) {
    child = lock_and_rebalance_left();
  } else {
    // The caller (locked_keyrange::acquire) handles the case where
    // the root of the locked_keyrange is the node that overlaps.
    // range is guaranteed not to overlap this node.
    invariant(c == keyrange::comparison::GREATER_THAN);
    child = lock_and_rebalance_right();
  }

  // if the search would lead us to an empty subtree (child == nullptr),
  // or the child overlaps, then we know this node is the parent we want.
  // otherwise we need to recur into that child.
  if (child == nullptr) {
    return this;
  } else {
    c = range.compare(*m_cmp, child->m_range);
    if (c == keyrange::comparison::EQUALS ||
        c == keyrange::comparison::OVERLAPS) {
      child->mutex_unlock();
      return this;
    } else {
      // unlock this node before recurring into the locked child,
      // passing in a comparison hint since we just comapred range
      // to the child's range.
      mutex_unlock();
      return child->find_node_with_overlapping_child(range, &c);
    }
  }
}

bool treenode::insert(const keyrange &range, TXNID txnid, bool is_shared) {
  int rc = true;
  // choose a child to check. if that child is null, then insert the new node
  // there. otherwise recur down that child's subtree
  keyrange::comparison c = range.compare(*m_cmp, m_range);
  if (c == keyrange::comparison::LESS_THAN) {
    treenode *left_child = lock_and_rebalance_left();
    if (left_child == nullptr) {
      left_child = treenode::alloc(m_cmp, range, txnid, is_shared);
      m_left_child.set(left_child);
    } else {
      rc = left_child->insert(range, txnid, is_shared);
      left_child->mutex_unlock();
    }
  } else if (c == keyrange::comparison::GREATER_THAN) {
    // invariant(c == keyrange::comparison::GREATER_THAN);
    treenode *right_child = lock_and_rebalance_right();
    if (right_child == nullptr) {
      right_child = treenode::alloc(m_cmp, range, txnid, is_shared);
      m_right_child.set(right_child);
    } else {
      rc = right_child->insert(range, txnid, is_shared);
      right_child->mutex_unlock();
    }
  } else if (c == keyrange::comparison::EQUALS) {
    invariant(is_shared);
    invariant(m_is_shared);
    rc = add_shared_owner(txnid);
  } else {
    invariant(0);
  }
  return rc;
}

treenode *treenode::find_child_at_extreme(int direction, treenode **parent) {
  treenode *child =
      direction > 0 ? m_right_child.get_locked() : m_left_child.get_locked();

  if (child) {
    *parent = this;
    treenode *child_extreme = child->find_child_at_extreme(direction, parent);
    child->mutex_unlock();
    return child_extreme;
  } else {
    return this;
  }
}

treenode *treenode::find_leftmost_child(treenode **parent) {
  return find_child_at_extreme(-1, parent);
}

treenode *treenode::find_rightmost_child(treenode **parent) {
  return find_child_at_extreme(1, parent);
}

treenode *treenode::remove_root_of_subtree() {
  // if this node has no children, just free it and return null
  if (m_left_child.ptr == nullptr && m_right_child.ptr == nullptr) {
    // treenode::free requires that non-root nodes are unlocked
    if (!is_root()) {
      mutex_unlock();
    }
    treenode::free(this);
    return nullptr;
  }

  // we have a child, so get either the in-order successor or
  // predecessor of this node to be our replacement.
  // replacement_parent is updated by the find functions as
  // they recur down the tree, so initialize it to this.
  treenode *child, *replacement;
  treenode *replacement_parent = this;
  if (m_left_child.ptr != nullptr) {
    child = m_left_child.get_locked();
    replacement = child->find_rightmost_child(&replacement_parent);
    invariant(replacement == child || replacement_parent != this);

    // detach the replacement from its parent
    if (replacement_parent == this) {
      m_left_child = replacement->m_left_child;
    } else {
      replacement_parent->m_right_child = replacement->m_left_child;
    }
  } else {
    child = m_right_child.get_locked();
    replacement = child->find_leftmost_child(&replacement_parent);
    invariant(replacement == child || replacement_parent != this);

    // detach the replacement from its parent
    if (replacement_parent == this) {
      m_right_child = replacement->m_right_child;
    } else {
      replacement_parent->m_left_child = replacement->m_right_child;
    }
  }
  child->mutex_unlock();

  // swap in place with the detached replacement, then destroy it
  treenode::swap_in_place(replacement, this);
  treenode::free(replacement);

  return this;
}

void treenode::recursive_remove(void) {
  treenode *left = m_left_child.ptr;
  if (left) {
    left->recursive_remove();
  }
  m_left_child.set(nullptr);

  treenode *right = m_right_child.ptr;
  if (right) {
    right->recursive_remove();
  }
  m_right_child.set(nullptr);

  // we do not take locks on the way down, so we know non-root nodes
  // are unlocked here and the caller is required to pass a locked
  // root, so this free is correct.
  treenode::free(this);
}

void treenode::remove_shared_owner(TXNID txnid) {
  assert(m_owners->size() > 1);
  m_owners->erase(txnid);
  assert(m_owners->size() > 0);
  /* if there is just one owner left, move it to m_txnid */
  if (m_owners->size() == 1) {
    m_txnid = *m_owners->begin();
    delete m_owners;
    m_owners = nullptr;
  }
}

treenode *treenode::remove(const keyrange &range, TXNID txnid) {
  treenode *child;
  // if the range is equal to this node's range, then just remove
  // the root of this subtree. otherwise search down the tree
  // in either the left or right children.
  keyrange::comparison c = range.compare(*m_cmp, m_range);
  switch (c) {
    case keyrange::comparison::EQUALS: {
      // if we are the only owners, remove. Otherwise, just remove
      // us from the owners list.
      if (txnid != TXNID_ANY && has_multiple_owners()) {
        remove_shared_owner(txnid);
        return this;
      } else {
        return remove_root_of_subtree();
      }
    }
    case keyrange::comparison::LESS_THAN:
      child = m_left_child.get_locked();
      invariant_notnull(child);
      child = child->remove(range, txnid);

      // unlock the child if there still is one.
      // regardless, set the right child pointer
      if (child) {
        child->mutex_unlock();
      }
      m_left_child.set(child);
      break;
    case keyrange::comparison::GREATER_THAN:
      child = m_right_child.get_locked();
      invariant_notnull(child);
      child = child->remove(range, txnid);

      // unlock the child if there still is one.
      // regardless, set the right child pointer
      if (child) {
        child->mutex_unlock();
      }
      m_right_child.set(child);
      break;
    case keyrange::comparison::OVERLAPS:
      // shouldn't be overlapping, since the tree is
      // non-overlapping and this range must exist
      abort();
  }

  return this;
}

bool treenode::left_imbalanced(int threshold) const {
  uint32_t left_depth = m_left_child.depth_est;
  uint32_t right_depth = m_right_child.depth_est;
  return m_left_child.ptr != nullptr && left_depth > threshold + right_depth;
}

bool treenode::right_imbalanced(int threshold) const {
  uint32_t left_depth = m_left_child.depth_est;
  uint32_t right_depth = m_right_child.depth_est;
  return m_right_child.ptr != nullptr && right_depth > threshold + left_depth;
}

// effect: rebalances the subtree rooted at this node
//         using AVL style O(1) rotations. unlocks this
//         node if it is not the new root of the subtree.
// requires: node is locked by this thread, children are not
// returns: locked root node of the rebalanced tree
treenode *treenode::maybe_rebalance(void) {
  // if we end up not rotating at all, the new root is this
  treenode *new_root = this;
  treenode *child = nullptr;

  if (left_imbalanced(IMBALANCE_THRESHOLD)) {
    child = m_left_child.get_locked();
    if (child->right_imbalanced(0)) {
      treenode *grandchild = child->m_right_child.get_locked();

      child->m_right_child = grandchild->m_left_child;
      grandchild->m_left_child.set(child);

      m_left_child = grandchild->m_right_child;
      grandchild->m_right_child.set(this);

      new_root = grandchild;
    } else {
      m_left_child = child->m_right_child;
      child->m_right_child.set(this);
      new_root = child;
    }
  } else if (right_imbalanced(IMBALANCE_THRESHOLD)) {
    child = m_right_child.get_locked();
    if (child->left_imbalanced(0)) {
      treenode *grandchild = child->m_left_child.get_locked();

      child->m_left_child = grandchild->m_right_child;
      grandchild->m_right_child.set(child);

      m_right_child = grandchild->m_left_child;
      grandchild->m_left_child.set(this);

      new_root = grandchild;
    } else {
      m_right_child = child->m_left_child;
      child->m_left_child.set(this);
      new_root = child;
    }
  }

  // up to three nodes may be locked.
  // - this
  // - child
  // - grandchild (but if it is locked, its the new root)
  //
  // one of them is the new root. we unlock everything except the new root.
  if (child && child != new_root) {
    TOKU_VALGRIND_RESET_MUTEX_ORDERING_INFO(&child->m_mutex);
    child->mutex_unlock();
  }
  if (this != new_root) {
    TOKU_VALGRIND_RESET_MUTEX_ORDERING_INFO(&m_mutex);
    mutex_unlock();
  }
  TOKU_VALGRIND_RESET_MUTEX_ORDERING_INFO(&new_root->m_mutex);
  return new_root;
}

treenode *treenode::lock_and_rebalance_left(void) {
  treenode *child = m_left_child.get_locked();
  if (child) {
    treenode *new_root = child->maybe_rebalance();
    m_left_child.set(new_root);
    child = new_root;
  }
  return child;
}

treenode *treenode::lock_and_rebalance_right(void) {
  treenode *child = m_right_child.get_locked();
  if (child) {
    treenode *new_root = child->maybe_rebalance();
    m_right_child.set(new_root);
    child = new_root;
  }
  return child;
}

void treenode::child_ptr::set(treenode *node) {
  ptr = node;
  depth_est = ptr ? ptr->get_depth_estimate() : 0;
}

treenode *treenode::child_ptr::get_locked(void) {
  if (ptr) {
    ptr->mutex_lock();
    depth_est = ptr->get_depth_estimate();
  }
  return ptr;
}

} /* namespace toku */
#endif  // OS_WIN
