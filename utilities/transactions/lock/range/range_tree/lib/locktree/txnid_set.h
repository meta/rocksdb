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

#include "../portability/txn_subst.h"
#include "../util/omt.h"

namespace toku {

class txnid_set {
 public:
  // effect: Creates an empty set. Does not malloc space for
  //         any entries yet. That is done lazily on add().
  void create(void);

  // effect: Destroy the set's internals.
  void destroy(void);

  // returns: True if the given txnid is a member of the set.
  bool contains(TXNID id) const;

  // effect: Adds a given txnid to the set if it did not exist
  void add(TXNID txnid);

  // effect: Deletes a txnid from the set if it exists.
  void remove(TXNID txnid);

  // returns: Size of the set
  uint32_t size(void) const;

  // returns: The "i'th" id in the set, as if it were sorted.
  TXNID get(uint32_t i) const;

 private:
  toku::omt<TXNID> m_txnids;

  friend class txnid_set_unit_test;
};
static_assert(std::is_trivially_default_constructible_v<txnid_set>);

} /* namespace toku */
