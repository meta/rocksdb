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

#include <functional>

#include "../util/omt.h"
#include "txnid_set.h"

namespace toku {

// A wfg is a 'wait-for' graph. A directed edge in represents one
// txn waiting for another to finish before it can acquire a lock.

class wfg {
 public:
  // Create a lock request graph
  void create(void);

  // Destroy the internals of the lock request graph
  void destroy(void);

  // Add an edge (a_id, b_id) to the graph
  void add_edge(TXNID a_txnid, TXNID b_txnid);

  // Return true if a node with the given transaction id exists in the graph.
  // Return false otherwise.
  bool node_exists(TXNID txnid);

  // Return true if there exists a cycle from a given transaction id in the
  // graph. Return false otherwise.
  bool cycle_exists_from_txnid(TXNID txnid,
                               std::function<void(TXNID)> reporter);

  // Apply a given function f to all of the nodes in the graph.  The apply
  // function returns when the function f is called for all of the nodes in the
  // graph, or the function f returns non-zero.
  void apply_nodes(int (*fn)(TXNID txnid, void *extra), void *extra);

  // Apply a given function f to all of the edges whose origin is a given node
  // id. The apply function returns when the function f is called for all edges
  // in the graph rooted at node id, or the function f returns non-zero.
  void apply_edges(TXNID txnid,
                   int (*fn)(TXNID txnid, TXNID edge_txnid, void *extra),
                   void *extra);

 private:
  struct node {
    // txnid for this node and the associated set of edges
    TXNID txnid;
    txnid_set edges;
    bool visited;

    static node *alloc(TXNID txnid);

    static void free(node *n);
  };
  static_assert(std::is_trivially_default_constructible_v<node>);

  toku::omt<node *> m_nodes;

  node *find_node(TXNID txnid);

  node *find_create_node(TXNID txnid);

  bool cycle_exists_from_node(node *target, node *head,
                              std::function<void(TXNID)> reporter);

  static int find_by_txnid(node *const &node_a, const TXNID &txnid_b);
};

} /* namespace toku */
