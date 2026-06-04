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

#ident \
    "Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved."

#include "lock_request.h"
#include <pthread.h>
#include <iostream>
#include <thread>
#include "locktree.h"
#include "test.h"

std::atomic_ulong n_retry_group_calls;
std::atomic_ulong n_retry_scans;
const int N = 32;
std::atomic_uint group_size[N];

static void test_callback([[maybe_unused]] const toku::txnid_set *completed_txns) {
    int r = completed_txns->size();
    if (r >= N)
        group_size[N-1]++;
    else
        group_size[r]++;
    n_retry_scans++;
}

namespace toku {
    static int n_tests = 1000;
    static int n_workers = 3;
    static int debug = 0;
    static int n_keys = 1;

    struct test_key {
        int64_t k;
        DBT dbt;
    };

    static test_key *keys = nullptr;

    static void run_locker(locktree_manager *mgr, locktree *lt, TXNID txnid) {
        int r;
        int n_waits = 0;
        for (int i = 0; i < n_tests; i++) {
            int rkey = random() % n_keys;

            lock_request request;
            request.create();
            request.set(lt, txnid, &keys[rkey].dbt, &keys[rkey].dbt, lock_request::type::WRITE, false);

            // try to acquire the lock
            r = request.start();
            if (r == DB_LOCK_NOTGRANTED) {
                n_waits += 1;
                // wait for the lock to be granted
                r = request.wait(1000 * 1000);
                if (r != 0)
                    std::cerr << std::this_thread::get_id() << " wait r=" << r << std::endl;
            }

            if (r == 0) {
                // do something with the key
                toku_pthread_yield();
                
                // release the lock
                range_buffer buffer;
                buffer.create();
                buffer.append(&keys[rkey].dbt, &keys[rkey].dbt);
                lt->release_locks(txnid, &buffer);
                buffer.destroy();

                // retry pending lock requests
                mgr->get_lock_request_info()->retry_lock_requests_group(txnid, test_callback);
                n_retry_group_calls++;
            }

            request.destroy();

            toku_pthread_yield();
            if (debug && (i % 10) == 0)
                std::cerr << std::this_thread::get_id() << " " << i
                          << std::endl;
        }

        std::cerr << std::this_thread::get_id() << " waits=" << n_waits << std::endl;
    }

} /* namespace toku */

static void print_usage(void) {
}

using namespace toku;

int main(int argc, char *argv[]) {
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-t") == 0 && i+1 < argc) {
            n_tests = atoi(argv[++i]);
            continue;
        }
        if (strcmp(argv[i], "-w") == 0 && i+1 < argc) {
            n_workers = atoi(argv[++i]);
            continue;
        }
        if (strcmp(argv[i], "-d") == 0 && i+1 < argc) {
            debug = atoi(argv[++i]);
            continue;
        }
        if (strcmp(argv[i], "-k") == 0 && i+1 < argc) {
            n_keys = atoi(argv[++i]);
            continue;
        }
        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-?") == 0) {
            print_usage();
            return 1;
        }
    }

    locktree_manager mgr;
    mgr.create(nullptr, nullptr, nullptr, nullptr);

    locktree lt;
    const DICTIONARY_ID dict_id = {1};
    lt.create(&mgr, dict_id, dbt_comparator);

    // init keys
    keys = new test_key[n_keys];
    for (int i=0; i<n_keys; i++) {
        keys[i].k = i;
        toku_fill_dbt(&keys[i].dbt, &keys[i].k, sizeof keys[i].k);
        keys[i].dbt.flags = DB_DBT_USERMEM;
    }

    // start workers
    std::thread worker[n_workers];
    for (int i = 0; i < n_workers; i++) {
        worker[i] = std::thread(run_locker, &mgr, &lt, i);
    }

    // cleanup workers
    for (int i = 0; i < n_workers; i++) {
        worker[i].join();
    }

    delete [] keys;
    lt.release_reference();
    lt.destroy();
    mgr.destroy();

    printf("n_retry_group_calls=%lu\n", n_retry_group_calls.load());
    printf("n_retry_scans=%lu\n", n_retry_scans.load());
    for (int i = 0; i < N; i++) {
        if (group_size[i] > 0)
            printf("group %d size %u\n", i, group_size[i].load());
    }
    return 0;
}
