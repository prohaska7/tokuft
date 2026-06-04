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

// Measure the performance of the lock retry versus the pending
// lock request set size.  The pending requests form a single dependency
// chain.

#include "lock_request_unit_test.h"

int n_lock_requests = 2;
int n_tests = 1;
int convert_to_tree = 1;
int do_iterate = 0;
int match = 0;

namespace toku {

    struct test_key {
        int64_t k;
        DBT dbt;
    };

    void lock_request_unit_test::run(void) {
        int r;

        // init manager and tree
        locktree_manager mgr;
        mgr.create(nullptr, nullptr, nullptr, nullptr);

        locktree *lt = mgr.get_lt({1}, dbt_comparator, nullptr);

        // init lock requests
        lock_request *lock_requests = new lock_request[n_lock_requests];
        for (int i=0; i<n_lock_requests; i++) {
            lock_requests[i].create();
        }

        // init keys
        test_key *keys = new test_key[n_lock_requests];
        for (int i=0; i<n_lock_requests; i++) {
            keys[i].k = i;
            toku_fill_dbt(&keys[i].dbt, &keys[i].k, sizeof keys[i].k);
            keys[i].dbt.flags = DB_DBT_USERMEM;
        }

        // lock keys
        for (int i=0; i<n_lock_requests; i++) {
            lock_requests[i].set(lt, (TXNID) {(uint64_t)(i+1)}, &keys[i].dbt, &keys[i].dbt,
                                 lock_request::type::WRITE, false);
            r = lock_requests[i].start();
            assert(r == 0);
            assert(lock_requests[i].m_state == lock_request::state::COMPLETE);
        }

        // try to lock adjacent keys
        for (int i=0; i<n_lock_requests-1; i++) {
            lock_requests[i].set(lt, (TXNID) {(uint64_t)(i+1)}, &keys[i+1].dbt, &keys[i+1].dbt,
                                 lock_request::type::WRITE, false);
            r = lock_requests[i].start();
            assert(r == DB_LOCK_NOTGRANTED);
            assert(lock_requests[i].m_state == lock_request::state::PENDING);
        }

        lock_request_info *lrmgr = mgr.get_lock_request_info();
        if (convert_to_tree)
            lrmgr->pending_lock_requests.convert_to_tree();

        // perf loop
        txnid_set completing_txnids;
        completing_txnids.create();
        if (match == 0)
            completing_txnids.add((TXNID) {(uint64_t)(n_lock_requests+1)}); // match none
        else if (match == 1)
            completing_txnids.add((TXNID) {(uint64_t)(0+1)}); // match one
        else
            completing_txnids.add(TXNID_NONE); // match all

        // perf loop
        for (int i=0; i<n_tests; i++) {
            if (do_iterate)
                lrmgr->retry_lock_requests_iterate(&completing_txnids);
            else
                lrmgr->retry_lock_requests_fetch(&completing_txnids);
        }

        completing_txnids.destroy();

        // complete lock requests
        for (int i=0; i<n_lock_requests-1; i++) {
            lrmgr->remove_from_pending(&lock_requests[i]);
            lock_requests[i].complete(DB_LOCK_NOTGRANTED);
        }

        // release locks
        for (int i=0; i<n_lock_requests; i++) {
            range_buffer buffer;
            buffer.create();
            buffer.append(&keys[i].dbt, &keys[i].dbt);
            lt->release_locks((TXNID) {(uint64_t)(i+1)}, &buffer);
            buffer.destroy();
        }

        // cleanup
        for (int i=0; i<n_lock_requests; i++) {
            lock_requests[i].destroy();
        }

        delete [] lock_requests;
        delete [] keys;

        mgr.release_lt(lt);
        mgr.destroy();
    }

} /* namespace toku */

static void print_usage(void) {
    fprintf(stderr, "-n N_LOCK_REQUESTS (default=%d)\n", n_lock_requests);
    fprintf(stderr, "-t N_TESTSS (default=%d)\n", n_tests);
    fprintf(stderr, "-c CONVERT_TO_TREE (default=%d)\n", convert_to_tree);
    fprintf(stderr, "-i DO_ITERATE (default=%d)\n", do_iterate);
    fprintf(stderr, "-m MATCH (default=%d)\n", match);
    fprintf(stderr, "0 -> retry none, 1 -> retry one, 2 -> retry all\n");
}

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
        if (strcmp(argv[i], "-m") == 0 && i+1 < argc) {
            match = atoi(argv[++i]);
            continue;
        }
        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-?") == 0) {
            print_usage();
            return 1;
        }
    }
    assert(n_lock_requests > 1);
    assert(n_tests > 0);
    toku::lock_request_unit_test test;
    test.run();
    return 0;
}

