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

// Measure the performance of the 'kill_waiter' implementation versus the pending
// lock request set size.

#include "lock_request_unit_test.h"

int n_lock_requests = 1;
int n_tests = 1;
// unfortunately, convert_to_tree function is private, but
// insert out of order forces omt convert to tree.
int convert_to_tree = 0;
int do_iterate = 0;
int do_pending = 1;

namespace toku {

    void lock_request_unit_test::run(void) {
        lock_request_info mgr;
        mgr.init();

        lock_request *lock_requests = new lock_request[n_lock_requests];
        long *extra = new long[n_lock_requests+1];
        int m = n_lock_requests/2;
        for (int i=0; i<n_lock_requests; i++) {
            lock_requests[i].create();
            lock_requests[i].m_txnid = i;
            extra[i] = random();
            lock_requests[i].m_info = &mgr;
            lock_requests[i].m_extra = &extra[i];
            if (!convert_to_tree || i != m)
                mgr.add_to_pending(&lock_requests[i]);
        }

        if (convert_to_tree)
            mgr.add_to_pending(&lock_requests[m]);

        for (int i=0; i<n_tests; i++) {
            long r = random() % n_lock_requests;
            void *kill_extra;
            if (do_pending)
                kill_extra = &extra[r];
            else
                kill_extra = &extra[n_lock_requests];
            if (do_iterate)
                mgr.kill_waiter_iterate(kill_extra);
            else
                mgr.kill_waiter_fetch(kill_extra);
            mgr.add_to_pending(&lock_requests[r]);
        }

        for (int i=0; i<n_lock_requests; i++)
            mgr.remove_from_pending(&lock_requests[i]);

        for (int i=0; i<n_lock_requests; i++)
            lock_requests[i].destroy();

        mgr.destroy();

        delete [] lock_requests;
        delete [] extra;
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
        if (strcmp(argv[i], "-c") == 0 && i+1 < argc) {
            convert_to_tree = atoi(argv[++i]);
            continue;
        }
        if (strcmp(argv[i], "-i") == 0 && i+1 < argc) {
            do_iterate = atoi(argv[++i]);
            continue;
        }
        if (strcmp(argv[i], "-p") == 0 && i+1 < argc) {
            do_pending = atoi(argv[++i]);
            continue;
        }
    }
    toku::lock_request_unit_test test;
    test.run();
    return 0;
}

