/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
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

#pragma once

#include <memory.h>
#include <stdint.h>

#include "../portability/toku_portability.h"
#include "../portability/toku_race_tools.h"
#include "growable_array.h"

namespace toku {

/**
 * Order Maintenance Tree (OMT)
 *
 * Maintains a collection of totally ordered values, where each value has an
 * integer weight. The OMT is a mutable datatype.
 *
 * The Abstraction:
 *
 * An OMT is a vector of values, $V$, where $|V|$ is the length of the vector.
 * The vector is numbered from $0$ to $|V|-1$.
 * Each value has a weight.  The weight of the $i$th element is denoted
 * $w(V_i)$.
 *
 * We can create a new OMT, which is the empty vector.
 *
 * We can insert a new element $x$ into slot $i$, changing $V$ into $V'$ where
 *  $|V'|=1+|V|$       and
 *
 *   V'_j = V_j       if $j<i$
 *          x         if $j=i$
 *          V_{j-1}   if $j>i$.
 *
 * We can specify $i$ using a kind of function instead of as an integer.
 * Let $b$ be a function mapping from values to nonzero integers, such that
 * the signum of $b$ is monotically increasing.
 * We can specify $i$ as the minimum integer such that $b(V_i)>0$.
 *
 * We look up a value using its index, or using a Heaviside function.
 * For lookups, we allow $b$ to be zero for some values, and again the signum of
 * $b$ must be monotonically increasing. When lookup up values, we can look up
 *  $V_i$ where $i$ is the minimum integer such that $b(V_i)=0$.   (With a
 * special return code if no such value exists.) (Rationale:  Ordinarily we want
 * $i$ to be unique.  But for various reasons we want to allow multiple zeros,
 * and we want the smallest $i$ in that case.) $V_i$ where $i$ is the minimum
 * integer such that $b(V_i)>0$.   (Or an indication that no such value exists.)
 *  $V_i$ where $i$ is the maximum integer such that $b(V_i)<0$.   (Or an
 * indication that no such value exists.)
 *
 * When looking up a value using a Heaviside function, we get the value and its
 * index.
 *
 * We can also split an OMT into two OMTs, splitting the weight of the values
 * evenly. Find a value $j$ such that the values to the left of $j$ have about
 * the same total weight as the values to the right of $j$. The resulting two
 * OMTs contain the values to the left of $j$ and the values to the right of $j$
 * respectively. All of the values from the original OMT go into one of the new
 * OMTs. If the weights of the values don't split exactly evenly, then the
 * implementation has the freedom to choose whether the new left OMT or the new
 * right OMT is larger.
 *
 * Performance:
 *  Insertion and deletion should run with $O(\log |V|)$ time and $O(\log |V|)$
 * calls to the Heaviside function. The memory required is O(|V|).
 *
 * Usage:
 *  The omt is templated by two parameters:
 *   - omtdata_t is what will be stored within the omt.  These could be pointers
 * or real data types (ints, structs).
 *   - omtdataout_t is what will be returned by find and related functions.  By
 * default, it is the same as omtdata_t, but you can set it to (omtdata_t *). To
 * create an omt which will store "TXNID"s, for example, it is a good idea to
 * typedef the template: typedef omt<TXNID> txnid_omt_t; If you are storing
 * structs, you may want to be able to get a pointer to the data actually stored
 * in the omt (see find_zero).  To do this, use the second template parameter:
 *   typedef omt<struct foo, struct foo *> foo_omt_t;
 */

namespace omt_internal {

template <bool subtree_supports_marks>
class subtree_templated {
 private:
  uint32_t m_index;

 public:
  static const uint32_t NODE_NULL = UINT32_MAX;
  inline void set_to_null(void) { m_index = NODE_NULL; }

  inline bool is_null(void) const { return NODE_NULL == this->get_index(); }

  inline uint32_t get_index(void) const { return m_index; }

  inline void set_index(uint32_t index) {
    paranoid_invariant(index != NODE_NULL);
    m_index = index;
  }
} __attribute__((__packed__, aligned(4)));

template <>
class subtree_templated<true> {
 private:
  uint32_t m_bitfield;
  static const uint32_t MASK_INDEX = ~(((uint32_t)1) << 31);
  static const uint32_t MASK_BIT = ((uint32_t)1) << 31;

  inline void set_index_internal(uint32_t new_index) {
    m_bitfield = (m_bitfield & MASK_BIT) | new_index;
  }

 public:
  static const uint32_t NODE_NULL = INT32_MAX;
  inline void set_to_null(void) { this->set_index_internal(NODE_NULL); }

  inline bool is_null(void) const { return NODE_NULL == this->get_index(); }

  inline uint32_t get_index(void) const {
    TOKU_DRD_IGNORE_VAR(m_bitfield);
    const uint32_t bits = m_bitfield;
    TOKU_DRD_STOP_IGNORING_VAR(m_bitfield);
    return bits & MASK_INDEX;
  }

  inline void set_index(uint32_t index) {
    paranoid_invariant(index < NODE_NULL);
    this->set_index_internal(index);
  }

  inline bool get_bit(void) const {
    TOKU_DRD_IGNORE_VAR(m_bitfield);
    const uint32_t bits = m_bitfield;
    TOKU_DRD_STOP_IGNORING_VAR(m_bitfield);
    return (bits & MASK_BIT) != 0;
  }

  inline void enable_bit(void) {
    // These bits may be set by a thread with a write lock on some
    // leaf, and the index can be read by another thread with a (read
    // or write) lock on another thread.  Also, the has_marks_below
    // bit can be set by two threads simultaneously.  Neither of these
    // are real races, so if we are using DRD we should tell it to
    // ignore these bits just while we set this bit.  If there were a
    // race in setting the index, that would be a real race.
    TOKU_DRD_IGNORE_VAR(m_bitfield);
    m_bitfield |= MASK_BIT;
    TOKU_DRD_STOP_IGNORING_VAR(m_bitfield);
  }

  inline void disable_bit(void) { m_bitfield &= MASK_INDEX; }
} __attribute__((__packed__));

template <typename omtdata_t, bool subtree_supports_marks>
class omt_node_templated {
 public:
  omtdata_t value;
  uint32_t weight;
  subtree_templated<subtree_supports_marks> left;
  subtree_templated<subtree_supports_marks> right;

  // this needs to be in both implementations because we don't have
  // a "static if" the caller can use
  inline void clear_stolen_bits(void) {}
};  // note: originally this class had __attribute__((__packed__, aligned(4)))

template <typename omtdata_t>
class omt_node_templated<omtdata_t, true> {
 public:
  omtdata_t value;
  uint32_t weight;
  subtree_templated<true> left;
  subtree_templated<true> right;
  inline bool get_marked(void) const { return left.get_bit(); }
  inline void set_marked_bit(void) { return left.enable_bit(); }
  inline void unset_marked_bit(void) { return left.disable_bit(); }

  inline bool get_marks_below(void) const { return right.get_bit(); }
  inline void set_marks_below_bit(void) {
    // This function can be called by multiple threads.
    // Checking first reduces cache invalidation.
    if (!this->get_marks_below()) {
      right.enable_bit();
    }
  }
  inline void unset_marks_below_bit(void) { right.disable_bit(); }

  inline void clear_stolen_bits(void) {
    this->unset_marked_bit();
    this->unset_marks_below_bit();
  }
};  // note: originally this class had __attribute__((__packed__, aligned(4)))

}  // namespace omt_internal

template <typename omtdata_t, typename omtdataout_t = omtdata_t,
          bool supports_marks = false>
class omt {
 public:
  /**
   * Effect: Create an empty OMT.
   * Performance: constant time.
   */
  void create(void);

  /**
   * Effect: Create an empty OMT with no internal allocated space.
   * Performance: constant time.
   * Rationale: In some cases we need a valid omt but don't want to malloc.
   */
  void create_no_array(void);

  /**
   * Effect: Create a OMT containing values.  The number of values is in
   * numvalues. Stores the new OMT in *omtp. Requires: this has not been created
   * yet Requires: values != NULL Requires: values is sorted Performance:
   * time=O(numvalues) Rationale:    Normally to insert N values takes O(N lg N)
   * amortized time. If the N values are known in advance, are sorted, and the
   * structure is empty, we can batch insert them much faster.
   */
  __attribute__((nonnull)) void create_from_sorted_array(
      const omtdata_t *const values, const uint32_t numvalues);

  /**
   * Effect: Create an OMT containing values.  The number of values is in
   * numvalues. On success the OMT takes ownership of *values array, and sets
   * values=NULL. Requires: this has not been created yet Requires: values !=
   * NULL Requires: *values is sorted Requires: *values was allocated with
   * toku_malloc Requires: Capacity of the *values array is <= new_capacity
   * Requires: On success, *values may not be accessed again by the caller.
   * Performance:  time=O(1)
   * Rational:     create_from_sorted_array takes O(numvalues) time.
   *               By taking ownership of the array, we save a malloc and
   * memcpy, and possibly a free (if the caller is done with the array).
   */
  void create_steal_sorted_array(omtdata_t **const values,
                                 const uint32_t numvalues,
                                 const uint32_t new_capacity);

  /**
   * Effect: Create a new OMT, storing it in *newomt.
   *  The values to the right of index (starting at index) are moved to *newomt.
   * Requires: newomt != NULL
   * Returns
   *    0             success,
   *    EINVAL        if index > toku_omt_size(omt)
   * On nonzero return, omt and *newomt are unmodified.
   * Performance: time=O(n)
   * Rationale:  We don't need a split-evenly operation.  We need to split items
   * so that their total sizes are even, and other similar splitting criteria.
   * It's easy to split evenly by calling size(), and dividing by two.
   */
  __attribute__((nonnull)) int split_at(omt *const newomt, const uint32_t idx);

  /**
   * Effect: Appends leftomt and rightomt to produce a new omt.
   *  Creates this as the new omt.
   *  leftomt and rightomt are destroyed.
   * Performance: time=O(n) is acceptable, but one can imagine implementations
   * that are O(\log n) worst-case.
   */
  __attribute__((nonnull)) void merge(omt *const leftomt, omt *const rightomt);

  /**
   * Effect: Creates a copy of an omt.
   *  Creates this as the clone.
   *  Each element is copied directly.  If they are pointers, the underlying
   * data is not duplicated. Performance: O(n) or the running time of
   * fill_array_with_subtree_values()
   */
  void clone(const omt &src);

  /**
   * Effect: Set the tree to be empty.
   *  Note: Will not reallocate or resize any memory.
   * Performance: time=O(1)
   */
  void clear(void);

  /**
   * Effect:  Destroy an OMT, freeing all its memory.
   *   If the values being stored are pointers, their underlying data is not
   * freed.  See free_items() Those values may be freed before or after calling
   * toku_omt_destroy. Rationale: Returns no values since free() cannot fail.
   * Rationale: Does not free the underlying pointers to reduce complexity.
   * Performance:  time=O(1)
   */
  void destroy(void);

  /**
   * Effect: return |this|.
   * Performance:  time=O(1)
   */
  uint32_t size(void) const;

  /**
   * Effect:  Insert value into the OMT.
   *   If there is some i such that $h(V_i, v)=0$ then returns DB_KEYEXIST.
   *   Otherwise, let i be the minimum value such that $h(V_i, v)>0$.
   *      If no such i exists, then let i be |V|
   *   Then this has the same effect as
   *    insert_at(tree, value, i);
   *   If idx!=NULL then i is stored in *idx
   * Requires:  The signum of h must be monotonically increasing.
   * Returns:
   *    0            success
   *    DB_KEYEXIST  the key is present (h was equal to zero for some value)
   * On nonzero return, omt is unchanged.
   * Performance: time=O(\log N) amortized.
   * Rationale: Some future implementation may be O(\log N) worst-case time, but
   * O(\log N) amortized is good enough for now.
   */
  template <typename omtcmp_t, int (*h)(const omtdata_t &, const omtcmp_t &)>
  int insert(const omtdata_t &value, const omtcmp_t &v, uint32_t *const idx);

  /**
   * Effect: Increases indexes of all items at slot >= idx by 1.
   *         Insert value into the position at idx.
   * Returns:
   *   0         success
   *   EINVAL    if idx > this->size()
   * On error, omt is unchanged.
   * Performance: time=O(\log N) amortized time.
   * Rationale: Some future implementation may be O(\log N) worst-case time, but
   * O(\log N) amortized is good enough for now.
   */
  int insert_at(const omtdata_t &value, const uint32_t idx);

  /**
   * Effect:  Replaces the item at idx with value.
   * Returns:
   *   0       success
   *   EINVAL  if idx>=this->size()
   * On error, omt is unchanged.
   * Performance: time=O(\log N)
   * Rationale: The FT needs to be able to replace a value with another copy of
   * the same value (allocated in a different location)
   *
   */
  int set_at(const omtdata_t &value, const uint32_t idx);

  /**
   * Effect: Delete the item in slot idx.
   *         Decreases indexes of all items at slot > idx by 1.
   * Returns
   *     0            success
   *     EINVAL       if idx>=this->size()
   * On error, omt is unchanged.
   * Rationale: To delete an item, first find its index using find or find_zero,
   * then delete it. Performance: time=O(\log N) amortized.
   */
  int delete_at(const uint32_t idx);

  /**
   * Effect:  Iterate over the values of the omt, from left to right, calling f
   * on each value. The first argument passed to f is a ref-to-const of the
   * value stored in the omt. The second argument passed to f is the index of
   * the value. The third argument passed to f is iterate_extra. The indices run
   * from 0 (inclusive) to this->size() (exclusive). Requires: f != NULL
   * Returns:
   *  If f ever returns nonzero, then the iteration stops, and the value
   * returned by f is returned by iterate. If f always returns zero, then
   * iterate returns 0. Requires:  Don't modify the omt while running.  (E.g., f
   * may not insert or delete values from the omt.) Performance: time=O(i+\log
   * N) where i is the number of times f is called, and N is the number of
   * elements in the omt. Rationale: Although the functional iterator requires
   * defining another function (as opposed to C++ style iterator), it is much
   * easier to read. Rationale: We may at some point use functors, but for now
   * this is a smaller change from the old OMT.
   */
  template <typename iterate_extra_t,
            int (*f)(const omtdata_t &, const uint32_t, iterate_extra_t *const)>
  int iterate(iterate_extra_t *const iterate_extra) const;

  /**
   * Effect:  Iterate over the values of the omt, from left to right, calling f
   * on each value. The first argument passed to f is a ref-to-const of the
   * value stored in the omt. The second argument passed to f is the index of
   * the value. The third argument passed to f is iterate_extra. The indices run
   * from 0 (inclusive) to this->size() (exclusive). We will iterate only over
   * [left,right)
   *
   * Requires: left <= right
   * Requires: f != NULL
   * Returns:
   *  EINVAL  if right > this->size()
   *  If f ever returns nonzero, then the iteration stops, and the value
   * returned by f is returned by iterate_on_range. If f always returns zero,
   * then iterate_on_range returns 0. Requires:  Don't modify the omt while
   * running.  (E.g., f may not insert or delete values from the omt.)
   * Performance: time=O(i+\log N) where i is the number of times f is called,
   * and N is the number of elements in the omt. Rational: Although the
   * functional iterator requires defining another function (as opposed to C++
   * style iterator), it is much easier to read.
   */
  template <typename iterate_extra_t,
            int (*f)(const omtdata_t &, const uint32_t, iterate_extra_t *const)>
  int iterate_on_range(const uint32_t left, const uint32_t right,
                       iterate_extra_t *const iterate_extra) const;

  /**
   * Effect: Iterate over the values of the omt, and mark the nodes that are
   * visited. Other than the marks, this behaves the same as iterate_on_range.
   * Requires: supports_marks == true
   * Performance: time=O(i+\log N) where i is the number of times f is called,
   * and N is the number of elements in the omt. Notes: This function MAY be
   * called concurrently by multiple threads, but not concurrently with any
   * other non-const function.
   */
  template <typename iterate_extra_t,
            int (*f)(const omtdata_t &, const uint32_t, iterate_extra_t *const)>
  int iterate_and_mark_range(const uint32_t left, const uint32_t right,
                             iterate_extra_t *const iterate_extra);

  /**
   * Effect: Iterate over the values of the omt, from left to right, calling f
   * on each value whose node has been marked. Other than the marks, this
   * behaves the same as iterate. Requires: supports_marks == true Performance:
   * time=O(i+\log N) where i is the number of times f is called, and N is the
   * number of elements in the omt.
   */
  template <typename iterate_extra_t,
            int (*f)(const omtdata_t &, const uint32_t, iterate_extra_t *const)>
  int iterate_over_marked(iterate_extra_t *const iterate_extra) const;

  /**
   * Effect: Delete all elements from the omt, whose nodes have been marked.
   * Requires: supports_marks == true
   * Performance: time=O(N + i\log N) where i is the number of marked elements,
   * {c,sh}ould be faster
   */
  void delete_all_marked(void);

  /**
   * Effect: Verify that the internal state of the marks in the tree are
   * self-consistent. Crashes the system if the marks are in a bad state.
   * Requires: supports_marks == true
   * Performance: time=O(N)
   * Notes:
   *  Even though this is a const function, it requires exclusive access.
   * Rationale:
   *  The current implementation of the marks relies on a sort of
   *  "cache" bit representing the state of bits below it in the tree.
   *  This allows glass-box testing that these bits are correct.
   */
  void verify_marks_consistent(void) const;

  /**
   * Effect: None
   * Returns whether there are any marks in the tree.
   */
  bool has_marks(void) const;

  /**
   * Effect:  Iterate over the values of the omt, from left to right, calling f
   * on each value. The first argument passed to f is a pointer to the value
   * stored in the omt. The second argument passed to f is the index of the
   * value. The third argument passed to f is iterate_extra. The indices run
   * from 0 (inclusive) to this->size() (exclusive). Requires: same as for
   * iterate() Returns: same as for iterate() Performance: same as for iterate()
   * Rationale: In general, most iterators should use iterate() since they
   * should not modify the data stored in the omt.  This function is for
   * iterators which need to modify values (for example, free_items). Rationale:
   * We assume if you are transforming the data in place, you want to do it to
   * everything at once, so there is not yet an iterate_on_range_ptr (but there
   * could be).
   */
  template <typename iterate_extra_t,
            int (*f)(omtdata_t *, const uint32_t, iterate_extra_t *const)>
  void iterate_ptr(iterate_extra_t *const iterate_extra);

  /**
   * Effect: Set *value=V_idx
   * Returns
   *    0             success
   *    EINVAL        if index>=toku_omt_size(omt)
   * On nonzero return, *value is unchanged
   * Performance: time=O(\log N)
   */
  int fetch(const uint32_t idx, omtdataout_t *const value) const;

  /**
   * Effect:  Find the smallest i such that h(V_i, extra)>=0
   *  If there is such an i and h(V_i,extra)==0 then set *idxp=i, set *value =
   * V_i, and return 0. If there is such an i and h(V_i,extra)>0  then set
   * *idxp=i and return DB_NOTFOUND. If there is no such i then set
   * *idx=this->size() and return DB_NOTFOUND. Note: value is of type
   * omtdataout_t, which may be of type (omtdata_t) or (omtdata_t *) but is
   * fixed by the instantiation. If it is the value type, then the value is
   * copied out (even if the value type is a pointer to something else) If it is
   * the pointer type, then *value is set to a pointer to the data within the
   * omt. This is determined by the type of the omt as initially declared. If
   * the omt is declared as omt<foo_t>, then foo_t's will be stored and foo_t's
   * will be returned by find and related functions. If the omt is declared as
   * omt<foo_t, foo_t *>, then foo_t's will be stored, and pointers to the
   * stored items will be returned by find and related functions. Rationale:
   *  Structs too small for malloc should be stored directly in the omt.
   *  These structs may need to be edited as they exist inside the omt, so we
   * need a way to get a pointer within the omt. Using separate functions for
   * returning pointers and values increases code duplication and reduces
   * type-checking. That also reduces the ability of the creator of a data
   * structure to give advice to its future users. Slight overloading in this
   * case seemed to provide a better API and better type checking.
   */
  template <typename omtcmp_t, int (*h)(const omtdata_t &, const omtcmp_t &)>
  int find_zero(const omtcmp_t &extra, omtdataout_t *const value,
                uint32_t *const idxp) const;

  /**
   *   Effect:
   *    If direction >0 then find the smallest i such that h(V_i,extra)>0.
   *    If direction <0 then find the largest  i such that h(V_i,extra)<0.
   *    (Direction may not be equal to zero.)
   *    If value!=NULL then store V_i in *value
   *    If idxp!=NULL then store i in *idxp.
   *   Requires: The signum of h is monotically increasing.
   *   Returns
   *      0             success
   *      DB_NOTFOUND   no such value is found.
   *   On nonzero return, *value and *idxp are unchanged
   *   Performance: time=O(\log N)
   *   Rationale:
   *     Here's how to use the find function to find various things
   *       Cases for find:
   *        find first value:         ( h(v)=+1, direction=+1 )
   *        find last value           ( h(v)=-1, direction=-1 )
   *        find first X              ( h(v)=(v< x) ? -1 : 1    direction=+1 )
   *        find last X               ( h(v)=(v<=x) ? -1 : 1    direction=-1 )
   *        find X or successor to X  ( same as find first X. )
   *
   *   Rationale: To help understand heaviside functions and behavor of find:
   *    There are 7 kinds of heaviside functions.
   *    The signus of the h must be monotonically increasing.
   *    Given a function of the following form, A is the element
   *    returned for direction>0, B is the element returned
   *    for direction<0, C is the element returned for
   *    direction==0 (see find_zero) (with a return of 0), and D is the element
   *    returned for direction==0 (see find_zero) with a return of DB_NOTFOUND.
   *    If any of A, B, or C are not found, then asking for the
   *    associated direction will return DB_NOTFOUND.
   *    See find_zero for more information.
   *
   *    Let the following represent the signus of the heaviside function.
   *
   *    -...-
   *        A
   *         D
   *
   *    +...+
   *    B
   *    D
   *
   *    0...0
   *    C
   *
   *    -...-0...0
   *        AC
   *
   *    0...0+...+
   *    C    B
   *
   *    -...-+...+
   *        AB
   *         D
   *
   *    -...-0...0+...+
   *        AC    B
   */
  template <typename omtcmp_t, int (*h)(const omtdata_t &, const omtcmp_t &)>
  int find(const omtcmp_t &extra, int direction, omtdataout_t *const value,
           uint32_t *const idxp) const;

  /**
   * Effect: Return the size (in bytes) of the omt, as it resides in main
   * memory.  If the data stored are pointers, don't include the size of what
   * they all point to.
   */
  size_t memory_size(void);

 private:
  typedef uint32_t node_idx;
  typedef omt_internal::subtree_templated<supports_marks> subtree;
  typedef omt_internal::omt_node_templated<omtdata_t, supports_marks> omt_node;
  static_assert(std::is_trivially_copy_constructible_v<subtree>
                    && std::is_trivially_copy_assignable_v<subtree>);

  struct omt_array {
    uint32_t start_idx;
    uint32_t num_values;
    omtdata_t *values;
  };

  struct omt_tree {
    subtree root;
    uint32_t free_idx;
    omt_node *nodes;
  };

  bool is_array;
  uint32_t capacity;
  union {
    struct omt_array a;
    struct omt_tree t;
  } d;

  __attribute__((nonnull)) void unmark(const subtree &subtree,
                                       const uint32_t index,
                                       GrowableArray<node_idx> *const indexes);

  void create_internal_no_array(const uint32_t new_capacity);

  void create_internal(const uint32_t new_capacity);

  uint32_t nweight(const subtree &subtree) const;

  node_idx node_malloc(void);

  void node_free(const node_idx idx);

  void maybe_resize_array(const uint32_t n);

  __attribute__((nonnull)) void fill_array_with_subtree_values(
      omtdata_t *const array, const subtree &subtree) const;

  void convert_to_array(void);

  __attribute__((nonnull)) void rebuild_from_sorted_array(
      subtree *const subtree, const omtdata_t *const values,
      const uint32_t numvalues);

  void convert_to_tree(void);

  void maybe_resize_or_convert(const uint32_t n);

  bool will_need_rebalance(const subtree &subtree, const int leftmod,
                           const int rightmod) const;

  __attribute__((nonnull)) void insert_internal(
      subtree *const subtreep, const omtdata_t &value, const uint32_t idx,
      subtree **const rebalance_subtree);

  void set_at_internal_array(const omtdata_t &value, const uint32_t idx);

  void set_at_internal(const subtree &subtree, const omtdata_t &value,
                       const uint32_t idx);

  void delete_internal(subtree *const subtreep, const uint32_t idx,
                       omt_node *const copyn,
                       subtree **const rebalance_subtree);

  template <typename iterate_extra_t,
            int (*f)(const omtdata_t &, const uint32_t, iterate_extra_t *const)>
  int iterate_internal_array(const uint32_t left, const uint32_t right,
                             iterate_extra_t *const iterate_extra) const;

  template <typename iterate_extra_t,
            int (*f)(omtdata_t *, const uint32_t, iterate_extra_t *const)>
  void iterate_ptr_internal(const uint32_t left, const uint32_t right,
                            const subtree &subtree, const uint32_t idx,
                            iterate_extra_t *const iterate_extra);

  template <typename iterate_extra_t,
            int (*f)(omtdata_t *, const uint32_t, iterate_extra_t *const)>
  void iterate_ptr_internal_array(const uint32_t left, const uint32_t right,
                                  iterate_extra_t *const iterate_extra);

  template <typename iterate_extra_t,
            int (*f)(const omtdata_t &, const uint32_t, iterate_extra_t *const)>
  int iterate_internal(const uint32_t left, const uint32_t right,
                       const subtree &subtree, const uint32_t idx,
                       iterate_extra_t *const iterate_extra) const;

  template <typename iterate_extra_t,
            int (*f)(const omtdata_t &, const uint32_t, iterate_extra_t *const)>
  int iterate_and_mark_range_internal(const uint32_t left, const uint32_t right,
                                      const subtree &subtree,
                                      const uint32_t idx,
                                      iterate_extra_t *const iterate_extra);

  template <typename iterate_extra_t,
            int (*f)(const omtdata_t &, const uint32_t, iterate_extra_t *const)>
  int iterate_over_marked_internal(const subtree &subtree, const uint32_t idx,
                                   iterate_extra_t *const iterate_extra) const;

  uint32_t verify_marks_consistent_internal(const subtree &subtree,
                                            const bool allow_marks) const;

  void fetch_internal_array(const uint32_t i, omtdataout_t *const value) const;

  void fetch_internal(const subtree &subtree, const uint32_t i,
                      omtdataout_t *const value) const;

  __attribute__((nonnull)) void fill_array_with_subtree_idxs(
      node_idx *const array, const subtree &subtree) const;

  __attribute__((nonnull)) void rebuild_subtree_from_idxs(
      subtree *const subtree, const node_idx *const idxs,
      const uint32_t numvalues);

  __attribute__((nonnull)) void rebalance(subtree *const subtree);

  __attribute__((nonnull)) static void copyout(omtdata_t *const out,
                                               const omt_node *const n);

  __attribute__((nonnull)) static void copyout(omtdata_t **const out,
                                               omt_node *const n);

  __attribute__((nonnull)) static void copyout(
      omtdata_t *const out, const omtdata_t *const stored_value_ptr);

  __attribute__((nonnull)) static void copyout(
      omtdata_t **const out, omtdata_t *const stored_value_ptr);

  template <typename omtcmp_t, int (*h)(const omtdata_t &, const omtcmp_t &)>
  int find_internal_zero_array(const omtcmp_t &extra, omtdataout_t *const value,
                               uint32_t *const idxp) const;

  template <typename omtcmp_t, int (*h)(const omtdata_t &, const omtcmp_t &)>
  int find_internal_zero(const subtree &subtree, const omtcmp_t &extra,
                         omtdataout_t *const value, uint32_t *const idxp) const;

  template <typename omtcmp_t, int (*h)(const omtdata_t &, const omtcmp_t &)>
  int find_internal_plus_array(const omtcmp_t &extra, omtdataout_t *const value,
                               uint32_t *const idxp) const;

  template <typename omtcmp_t, int (*h)(const omtdata_t &, const omtcmp_t &)>
  int find_internal_plus(const subtree &subtree, const omtcmp_t &extra,
                         omtdataout_t *const value, uint32_t *const idxp) const;

  template <typename omtcmp_t, int (*h)(const omtdata_t &, const omtcmp_t &)>
  int find_internal_minus_array(const omtcmp_t &extra,
                                omtdataout_t *const value,
                                uint32_t *const idxp) const;

  template <typename omtcmp_t, int (*h)(const omtdata_t &, const omtcmp_t &)>
  int find_internal_minus(const subtree &subtree, const omtcmp_t &extra,
                          omtdataout_t *const value,
                          uint32_t *const idxp) const;
};

}  // namespace toku

// include the implementation here
#include "omt_impl.h"
