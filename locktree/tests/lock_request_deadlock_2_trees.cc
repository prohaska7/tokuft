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
======= */

#ident "Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved."

#include "lock_request_unit_test.h"

namespace toku {

// Verify that a deadlock is detected for a simple locking situation with 2 transactions
// and 2 keys on 2 lock trees.
// txn_a locks lt_1 key_alpha
// txn_b locks lt_2 key_beta
// txn_a locks lt_2 key_beta, should get pending error
// txn_b locks lt_1 key_alpha, should get deadlock error

void lock_request_unit_test::run(void) {
    int r;

    locktree_manager mgr;
    mgr.create(nullptr, nullptr, nullptr, nullptr);

    locktree lt_1;
    lt_1.create(&mgr, (DICTIONARY_ID){1}, dbt_comparator);

    locktree lt_2;
    lt_2.create(&mgr, (DICTIONARY_ID){2}, dbt_comparator);

    const DBT *key_alpha = get_dbt(1);
    const DBT *key_beta = get_dbt(2);

    const TXNID txnid_a = 1001;
    const TXNID txnid_b = 2001;

    lock_request request_a;
    request_a.create();

    lock_request request_b;
    request_b.create();

    // txnid_a locks lt_1 key_alpha
    request_a.set(&lt_1, txnid_a, key_alpha, key_alpha, lock_request::type::WRITE, false);
    r = request_a.start();
    invariant_zero(r);

    // txnid_b locks lt_2 key_beta
    request_b.set(&lt_2, txnid_b, key_beta, key_beta, lock_request::type::WRITE, false);
    r = request_b.start();
    invariant_zero(r);

    // txnid_a tries to lock lt_2 key_beta, conflicts with txnid_b, lock request should be pending
    request_a.set(&lt_2, txnid_a, key_beta, key_beta, lock_request::type::WRITE, false);
    r = request_a.start();
    invariant(r == DB_LOCK_NOTGRANTED);
    invariant(request_a.m_state == lock_request::state::PENDING);

    // txnid_b tries to lock lt_1 key_alpha, conflicts with txnid_b, should get deadlock error
    request_b.set(&lt_1, txnid_b, key_alpha, key_alpha, lock_request::type::WRITE, false);
    r = request_b.start();
    invariant(r == DB_LOCK_DEADLOCK);

    invariant(request_b.m_state == lock_request::state::COMPLETE);
    invariant(request_b.m_complete_r == DB_LOCK_DEADLOCK);

    // release locks for txnid_b
    release_lock_and_retry_requests(&lt_2, txnid_b, key_beta, key_beta);
    invariant(request_a.m_state == lock_request::state::COMPLETE);
    invariant(request_a.m_complete_r == 0);

    // release locks for txnid_a
    release_lock_and_retry_requests(&lt_2, txnid_a, key_beta, key_beta);
    release_lock_and_retry_requests(&lt_1, txnid_a, key_alpha, key_alpha);

    request_a.destroy();
    request_b.destroy();

    lt_1.release_reference();
    lt_1.destroy();
    lt_2.release_reference();
    lt_2.destroy();

    mgr.destroy();
}

} /* namespace toku */

int main(void) {
    toku::lock_request_unit_test test;
    test.run();
    return 0;
}

