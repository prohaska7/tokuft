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

// Measure the performance of the deadlock detector versus the pending
// lock request set size. The pending requests form a single dependency
// chain.

#include "lock_request_unit_test.h"

int n_lock_requests = 1;
int n_tests = 1;

namespace toku {

    void lock_request_unit_test::run(void) {
        lock_request_info mgr;
        mgr.init();

        lock_request *lock_requests = new lock_request[n_lock_requests];
        for (int i=0; i<n_lock_requests; i++) {
            lock_requests[i].create();
        }

        // Add 0->1, 1->2, 2->3, ... N-2->N-1
        for (int i=0; i<n_lock_requests-1; i++) {
            lock_requests[i].m_txnid = i;
            lock_requests[i].m_conflicts.add(i+1);
            mgr.add_to_pending(&lock_requests[i]);
            assert(!mgr.deadlock_exists(&lock_requests[i]));
        }

        // Add N-1 -> 0
        lock_requests[n_lock_requests-1].m_txnid = n_lock_requests-1;
        lock_requests[n_lock_requests-1].m_conflicts.add(0);
        mgr.add_to_pending(&lock_requests[n_lock_requests-1]);
        for (int i=0; i<n_lock_requests; i++) {
            assert(mgr.deadlock_exists(&lock_requests[i]));
        }

        // perf loop
        for (int i=0; i<n_tests; i++) {
            assert(mgr.deadlock_exists(&lock_requests[0]));
        }

        // cleanup
        for (int i=0; i<n_lock_requests; i++) {
            mgr.remove_from_pending(&lock_requests[i]);
        }

        for (int i=0; i<n_lock_requests; i++) {
            lock_requests[i].destroy();
        }

        mgr.destroy();
        delete [] lock_requests;
    }

} /* namespace toku */

int main(int argc, char *argv[]) {
    for (int i=1; i<argc; i++) {
        if (strcmp(argv[i], "-n") == 0 && i+1 < argc) {
            n_lock_requests = atoi(argv[++i]);
            continue;
        }
        if (strcmp(argv[i], "-t") == 0 && i+1 < argc) {
            n_tests = atoi(argv[++i]);
            continue;
        }
    }
    toku::lock_request_unit_test test;
    test.run();
    return 0;
}

