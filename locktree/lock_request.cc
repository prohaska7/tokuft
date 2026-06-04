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

#ident "Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved."

#include "portability/toku_race_tools.h"

#include "ft/txn/txn.h"
#include "locktree/locktree.h"
#include "locktree/lock_request.h"
#include "util/dbt.h"

namespace toku {

// initialize a lock request's internals
void lock_request::create(void) {
    m_txnid = TXNID_NONE;
    m_conflicts.create();
    m_start_time = 0;
    m_left_key = nullptr;
    m_right_key = nullptr;
    toku_init_dbt(&m_left_key_copy);
    toku_init_dbt(&m_right_key_copy);

    m_type = type::UNKNOWN;
    m_lt = nullptr;

    m_complete_r = 0;
    m_state = state::UNINITIALIZED;
    m_info = nullptr;

    toku_cond_init(*lock_request_m_wait_cond_key, &m_wait_cond, nullptr);

    m_start_test_callback = nullptr;
    m_start_before_pending_test_callback = nullptr;
    m_retry_test_callback = nullptr;
    m_wfg_visited = false;
}

// destroy a lock request.
void lock_request::destroy(void) {
    invariant(m_state != state::PENDING);
    invariant(m_state != state::DESTROYED);
    m_state = state::DESTROYED;
    m_conflicts.destroy();
    toku_destroy_dbt(&m_left_key_copy);
    toku_destroy_dbt(&m_right_key_copy);
    toku_cond_destroy(&m_wait_cond);
}

// set the lock request parameters. this API allows a lock request to be reused.
void lock_request::set(locktree *lt, TXNID txnid, const DBT *left_key, const DBT *right_key, lock_request::type lock_type, bool big_txn, void *extra) {
    invariant(m_state != state::PENDING);
    m_lt = lt;
    m_txnid = txnid;
    m_left_key = left_key;
    m_right_key = right_key;
    toku_destroy_dbt(&m_left_key_copy);
    toku_destroy_dbt(&m_right_key_copy);
    m_type = lock_type;
    m_state = state::INITIALIZED;
    m_info = lt ? lt->get_manager()->get_lock_request_info() : nullptr;
    m_big_txn = big_txn;
    m_extra = extra;
}

// get rid of any stored left and right key copies and
// replace them with copies of the given left and right key
void lock_request::copy_keys() {
    if (!toku_dbt_is_infinite(m_left_key)) {
        toku_clone_dbt(&m_left_key_copy, *m_left_key);
        m_left_key = &m_left_key_copy;
    }
    if (!toku_dbt_is_infinite(m_right_key)) {
        toku_clone_dbt(&m_right_key_copy, *m_right_key);
        m_right_key = &m_right_key_copy;
    }
}

// try to acquire a lock described by this lock request. 
int lock_request::start(void) {
    int r;

    m_conflicts.clear();
    if (m_type == type::WRITE) {
        r = m_lt->acquire_write_lock(m_txnid, m_left_key, m_right_key, &m_conflicts, m_big_txn);
    } else {
        invariant(m_type == type::READ);
        r = m_lt->acquire_read_lock(m_txnid, m_left_key, m_right_key, &m_conflicts, m_big_txn);
    }

    // if the lock is not granted, save it to the set of lock requests
    // and check for a deadlock. if there is one, complete it as failed
    if (r == DB_LOCK_NOTGRANTED) {
        copy_keys();
        m_state = state::PENDING;
        m_start_time = toku_current_time_microsec() / 1000;
        if (m_start_before_pending_test_callback)
            m_start_before_pending_test_callback();
        toku_mutex_lock(&m_info->mutex);
        m_info->add_to_pending(this);
        if (m_info->deadlock_exists(this)) {
            m_info->remove_from_pending(this);
            r = DB_LOCK_DEADLOCK;
        }
        toku_mutex_unlock(&m_info->mutex);
        if (m_start_test_callback)
            m_start_test_callback();  // test callback
    }

    if (r != DB_LOCK_NOTGRANTED) {
        complete(r);
    }

    return r;
}

// sleep on the lock request until it becomes resolved or the wait time has elapsed.
int lock_request::wait(uint64_t wait_time_ms) {
    return wait(wait_time_ms, 0, nullptr);
}

int lock_request::wait(uint64_t wait_time_ms, uint64_t killed_time_ms, int (*killed_callback)(void)) {
    uint64_t t_now = toku_current_time_microsec();
    uint64_t t_start = t_now;
    uint64_t t_end = t_start + wait_time_ms * 1000;

    toku_mutex_lock(&m_info->mutex);

    // check again, this time locking out other retry calls
    if (m_state == state::PENDING) {
        retry();
    }

    while (m_state == state::PENDING) {
        // check if this thread is killed
        if (killed_callback && killed_callback()) {
            m_info->remove_from_pending(this);
            complete(DB_LOCK_NOTGRANTED);
            continue;
        }

        // compute next wait time
        uint64_t t_wait;
        if (killed_time_ms == 0) {
            t_wait = t_end;
        } else {
            t_wait = t_now + killed_time_ms * 1000;
            if (t_wait > t_end)
                t_wait = t_end;
        }
        struct timespec ts = {};
        ts.tv_sec = t_wait / 1000000;
        ts.tv_nsec = (t_wait % 1000000) * 1000;
        int r = toku_cond_timedwait(&m_wait_cond, &m_info->mutex, &ts);
        invariant(r == 0 || r == ETIMEDOUT);

        t_now = toku_current_time_microsec();
        if (m_state == state::PENDING && (t_now >= t_end)) {
            m_info->counters.timeout_count += 1;

            // if we're still pending and we timed out, then remove our
            // request from the set of lock requests and fail.
            m_info->remove_from_pending(this);

            // complete sets m_state to COMPLETE, breaking us out of the loop
            complete(DB_LOCK_NOTGRANTED);
        }
    }

    uint64_t t_real_end = toku_current_time_microsec();
    uint64_t duration = t_real_end - t_start;
    m_info->counters.wait_count += 1;
    m_info->counters.wait_time += duration;
    if (duration >= 1000000) {
        m_info->counters.long_wait_count += 1;
        m_info->counters.long_wait_time += duration;
    }
    toku_mutex_unlock(&m_info->mutex);

    invariant(m_state == state::COMPLETE);
    return m_complete_r;
}

// complete this lock request with the given return value
void lock_request::complete(int complete_r) {
    m_complete_r = complete_r;
    m_state = state::COMPLETE;
}

const DBT *lock_request::get_left_key(void) const {
    return m_left_key;
}

const DBT *lock_request::get_right_key(void) const {
    return m_right_key;
}

TXNID lock_request::get_txnid(void) const {
    return m_txnid;
}

uint64_t lock_request::get_start_time(void) const {
    return m_start_time;
}

TXNID lock_request::get_conflicting_txnid(void) const {
    assert(m_conflicts.size() > 0);
    return m_conflicts.get(0);
}

const txnid_set *lock_request::get_conflicts(void) const {
    return &m_conflicts;
}

int lock_request::retry(void) {
    invariant(m_state == state::PENDING);
    int r;

    m_conflicts.clear();
    if (m_type == type::WRITE) {
        r = m_lt->acquire_write_lock(
            m_txnid, m_left_key, m_right_key, &m_conflicts, m_big_txn);
    } else {
        r = m_lt->acquire_read_lock(
            m_txnid, m_left_key, m_right_key, &m_conflicts, m_big_txn);
    }

    // if the acquisition succeeded or if out of locks
    // then remove ourselves from the set of lock requests, complete
    // the lock request, and signal the waiting threads.
    if (r == 0 || r == TOKUDB_OUT_OF_LOCKS) {
        m_info->remove_from_pending(this);
        complete(r);
        if (m_retry_test_callback)
            m_retry_test_callback();  // test callback
        toku_cond_broadcast(&m_wait_cond);
    } else if (r == DB_LOCK_NOTGRANTED) {
        ; // remain pending and do nothing
    } else {
        invariant(0);
    }

    return r;
}

void *lock_request::get_extra(void) const {
    return m_extra;
}

void lock_request::kill_waiter(void) {
    m_info->remove_from_pending(this);
    complete(DB_LOCK_NOTGRANTED);
    toku_cond_broadcast(&m_wait_cond);
}

void lock_request::set_start_test_callback(void (*f)(void)) {
    m_start_test_callback = f;
}

void lock_request::set_start_before_pending_test_callback(void (*f)(void)) {
    m_start_before_pending_test_callback = f;
}

void lock_request::set_retry_test_callback(void (*f)(void)) {
    m_retry_test_callback = f;
}

void lock_request_info::init(void) {
    pending_lock_requests.create();
    pending_is_empty = true;
    ZERO_STRUCT(mutex);
    toku_mutex_init(*locktree_request_info_mutex_key, &mutex, nullptr);
    retry_set = 0;
    retry_running = false;
    retry_waiting = false;
    for (int i=0; i<2; i++)
        retry_complete_set[i].create();
    ZERO_STRUCT(counters);
    ZERO_STRUCT(retry_mutex);
    toku_mutex_init(
        *locktree_request_info_retry_mutex_key, &retry_mutex, nullptr);
    toku_cond_init(*locktree_request_info_retry_cv_key, &retry_cv, nullptr);
    TOKU_VALGRIND_HG_DISABLE_CHECKING(&pending_is_empty,
                                      sizeof(pending_is_empty));
    TOKU_DRD_IGNORE_VAR(pending_is_empty);
}

void lock_request_info::destroy(void) {
    for (int i=0; i<2; i++) {
        invariant(retry_complete_set[i].size() == 0);
        retry_complete_set[i].destroy();
    }
    invariant(pending_lock_requests.size() == 0);
    pending_lock_requests.destroy();
    toku_mutex_destroy(&mutex);
    toku_mutex_destroy(&retry_mutex);
    toku_cond_destroy(&retry_cv);
}

// Invoke callback function on all lock requests in the pending lock request set.
// If the callback function returns non-zero, then the iteration is complete.
// Performance note: use an omt iterator since it has less complexity O(log n)
// compared to this implementation O(n log n).
int lock_request_info::iterate_pending_lock_requests(lock_request_iterate_callback callback,
                                                     void *extra) {
    int r = 0;
    toku_mutex_lock(&mutex);
    size_t num_requests = pending_lock_requests.size();
    for (size_t i = 0; i < num_requests && r == 0; i++) {
        lock_request *req;
        r = pending_lock_requests.fetch(i, &req);
        invariant_zero(r);
        r = callback(req->m_lt->get_dict_id(), req->get_txnid(),
                     req->get_left_key(), req->get_right_key(),
                     req->get_conflicting_txnid(), req->get_start_time(), extra);
    }

    toku_mutex_unlock(&mutex);
    return r;
}

// Find a lock request in the set of pending lock request that matches
// 'extra' and kill it. The 'kill_waiter_iterate' is faster than
// 'kill_waiter_fetch' since the cost of fetch is O(log n).
// If an unordered map from 'extra' to 'lock_request *' were added, then
// the performance could be O(1) at the cost of more code.
void lock_request_info::kill_waiter(void *extra) {
    kill_waiter_iterate(extra);
}

// Performance: O(n log n) since OMT fetch is O(log n)
void lock_request_info::kill_waiter_fetch(void *extra) {
    toku_mutex_lock(&mutex);
    for (size_t i = 0; i < pending_lock_requests.size(); i++) {
        lock_request *request;
        int r = pending_lock_requests.fetch(i, &request);
        if (r == 0 && request->get_extra() == extra) {
            request->kill_waiter();
            break;
        }
    }
    toku_mutex_unlock(&mutex);
}

// kill_waiter iterator data
struct kill_waiter_match_d {
    void *target_extra;
    uint32_t found_idx;
    lock_request *found_request;
};

// kill_waiter iterator callback function
static int kill_waiter_match_f(lock_request * const &request, const uint32_t idx, kill_waiter_match_d *const match) {
    if (request->get_extra() == match->target_extra) {
        match->found_idx = idx;
        match->found_request = request;
        return 1;
    } else
        return 0;
}

// Performance: O(n + log n) since the OMT iterate is O(n + log n)
void lock_request_info::kill_waiter_iterate(void *extra) {
    toku_mutex_lock(&mutex);
    kill_waiter_match_d match = { extra };
    int r = pending_lock_requests.iterate<kill_waiter_match_d, kill_waiter_match_f>(&match);
    if (r) {
        match.found_request->kill_waiter();
    }
    toku_mutex_unlock(&mutex);
}

void lock_request_info::get_status(uint64_t *lock_requests_pending_ptr, lock_request_counters *counters_ptr) {
    if (toku_mutex_trylock(&mutex) == 0) {
        *lock_requests_pending_ptr = pending_lock_requests.size();
        *counters_ptr = counters;
        toku_mutex_unlock(&mutex);
    } else {
        *lock_requests_pending_ptr = 0;
        *counters_ptr = {};
    }
}

// Add a lock request into the set of pending lock requests.
void lock_request_info::add_to_pending(lock_request *request) {
    uint32_t idx;
    int r = pending_lock_requests.find_zero<TXNID, find_by_txnid>(
            request->get_txnid(), &request, &idx);
    invariant(r == DB_NOTFOUND);
    r = pending_lock_requests.insert_at(request, idx);
    invariant_zero(r);
    pending_is_empty = false;
}

// Remove a lock request from the set of pending lock requests.
void lock_request_info::remove_from_pending(lock_request *request) {
    uint32_t idx;
    lock_request *found_lock_request;
    int r = pending_lock_requests.find_zero<TXNID, find_by_txnid>(
            request->get_txnid(), &found_lock_request, &idx);
    invariant_zero(r);
    invariant(request == found_lock_request);
    r = pending_lock_requests.delete_at(idx);
    invariant_zero(r);
    if (pending_lock_requests.size() == 0)
        pending_is_empty = true;
}

int lock_request_info::find_by_txnid(lock_request *const &request,
                                const TXNID &txnid) {
    TXNID request_txnid = request->get_txnid();
    if (request_txnid < txnid) {
        return -1;
    } else if (request_txnid == txnid) {
        return 0;
    } else {
        return 1;
    }
}

// Determine if a deadlock exists in the wait for graph contained in
// the set of pending lock requests.  If a cycle exists in the graph,
// then a deadlock exists.  This algorithm does a search rooted in
// a given lock request called lr.
// Return true if there is a deadlock in the wait for graph involving
// the given lock request.  Otherwise returns false.
// Requires lock_request_info mutex locked.
bool lock_request_info::deadlock_exists(lock_request *lr) {
    bool found_deadlock = false;
    for (size_t i = 0; i < lr->m_conflicts.size(); i++) {
        TXNID conflicting_txnid = lr->m_conflicts.get(i);
        found_deadlock = cycle_exists(conflicting_txnid, lr->m_txnid);
        if (found_deadlock)
            break;
    }
    return found_deadlock;
}

// Determine if a cycle exists in the wait for graph from 'from' to
// 'target'. Find the lock request for txnid 'from'.  If there is none,
// then there is no cycle.  If it was already visited, then there
// is a cycle in the graph.  If the lock request has the target
// txnid as one of its conflicts, then a cycle exist.  Otherwise,
// recursively see if a cycle exists from each conflict to the target.
// Return true if there is a cycle.  Otherwise returns false.
bool lock_request_info::cycle_exists(TXNID from, TXNID target) {
    bool found_cycle;
    lock_request *lr;
    int r = pending_lock_requests.find_zero<TXNID, find_by_txnid>(from, &lr, nullptr);
    if (r != 0) {
        found_cycle = false;
    } else if (lr->m_wfg_visited) {
        found_cycle = true;
    } else {
        if (lr->m_conflicts.contains(target)) {
            found_cycle = true;
        } else {
            lr->m_wfg_visited = true;
            for (size_t i = 0; i < lr->m_conflicts.size(); i++) {
                TXNID conflicting_txnid = lr->m_conflicts.get(i);
                found_cycle = cycle_exists(conflicting_txnid, target);
                if (found_cycle)
                    break;
            }
            lr->m_wfg_visited = false;
        }
    }
    return found_cycle;
}

// Retry pending lock requests with groups of completing transaction id's.
// This amortizes the cost of reassigning lock ownership to the appropriate
// pending lock requests over multiple completing transactions.
// This implementation uses 2 sets of completing transactions id's: one set
// is currently being used for lock retries and the other set used to accumulate
// completing transactions id's for the next cycle of lock retries.
// The lock retries are done by one of the calling threads that grabs
// the 'retry_running' flag first.  A second thread that grabs the 'retry_waiting'
// flag will become the next running thread.  All other threads deposit
// their transaction id into the next retry completion set.
// Alternative design: feed the lock retry work to a set of background threads.
void lock_request_info::retry_lock_requests_group(TXNID completing_txnid,
    void (*after_retry_test_callback)(const txnid_set *)) {

    // if there are no pending lock requests than there is nothing to do
    // the unlocked data race on pending_is_empty is OK since lock requests
    // are retried after added to the pending set.
    if (pending_is_empty) {
        // printf("e");
        return;
    }

    // Test
    if (0) {
        retry_lock_requests(nullptr);
        return;
    }

    toku_mutex_lock(&retry_mutex);

    // Add the completing transaction id to the next retry set.
    retry_complete_set[retry_set].add(completing_txnid);

    // Try to run the lock retries.
    if (retry_running && !retry_waiting) {
            // printf(".");
            retry_waiting = true;
            toku_cond_wait(&retry_cv, &retry_mutex);
            retry_waiting = false;
    }
    
    // printf("s");
    if (!retry_running && !retry_waiting) {
        retry_running = true;
        txnid_set *completed_txns = &retry_complete_set[retry_set];
        retry_set ^= 1; // switch completion sets
        toku_mutex_unlock(&retry_mutex);
        
        retry_lock_requests(completed_txns);
        if (after_retry_test_callback)
            after_retry_test_callback(completed_txns);
        
        toku_mutex_lock(&retry_mutex);
        // printf("d");
        completed_txns->clear();
        if (retry_waiting) {
            toku_cond_broadcast(&retry_cv);
        }
        retry_running = false;
    } else {
        ; // printf("0");
    }
    toku_mutex_unlock(&retry_mutex);
}

void lock_request_info::retry_lock_requests(txnid_set *completing_txnids) {
    retry_lock_requests_iterate(completing_txnids);
}

// Retry pending lock requests using an OMT fetch loop.  The lock requests selected
// are those whose conflicts contains one of the completing transactions id's.
// Performance: O(n log n) since OMT fetch is O(n).
void lock_request_info::retry_lock_requests_fetch(txnid_set *completing_txnids) {
    toku_mutex_lock(&mutex);
    for (size_t i = 0; i < pending_lock_requests.size();) {
        lock_request *request;
        int r = pending_lock_requests.fetch(i, &request);
        invariant_zero(r);

        bool do_retry;
        if (completing_txnids == nullptr) {
            // retry all
            do_retry = true;
        } else {
            // retry requests with conflicts with any completing_txnids
            do_retry = false;
            for (size_t j = 0; j < completing_txnids->size(); j++) {
                TXNID completing_txnid = completing_txnids->get(j);
                if (completing_txnid == TXNID_NONE || request->m_conflicts.contains(completing_txnid)) {
                    do_retry = true;
                    break;
                }
            }
        }
        if (do_retry) {
            // retry the lock request. if it didn't succeed,
            // move on to the next lock request. otherwise
            // the request is gone from the list so we may
            // read the i'th entry for the next one.
            r = request->retry();
            if (r != 0) {
                i++;
            }
        } else
            i++; // filtering request, go to the next one
    }

    toku_mutex_unlock(&mutex);
}

struct retry_lock_match_d {
    txnid_set *completing_txnids;
    GrowableArray<lock_request *> reqs;
};

static int retry_lock_match_f(lock_request * const &request, const uint32_t UU(idx), retry_lock_match_d *const match) {
    bool do_retry;
    if (match->completing_txnids == nullptr) {
        // retry all
        do_retry = true;
    } else {
        // retry requests with conflicts with any completing_txnids
        do_retry = false;
        for (size_t i = 0; i < match->completing_txnids->size(); i++) {
            TXNID completing_txnid = match->completing_txnids->get(i);
            if (completing_txnid == TXNID_NONE || request->get_conflicts()->contains(completing_txnid)) {
                do_retry = true;
                break;
            }
        }
    }
    if (do_retry) {
        match->reqs.push(request);
    }
    return 0;
}

// Retry pending lock requests using an OMT iterator.  The lock requests selected
// are those whose conflicts contains one of the completing transactions id's.
// Performance: O(n + log n) since OMT iterate is O(n + log n).
void lock_request_info::retry_lock_requests_iterate(txnid_set *completing_txnids) {
    // find matching lock requests
    retry_lock_match_d match;
    match.completing_txnids = completing_txnids;
    match.reqs.init();

    toku_mutex_lock(&mutex);
    int r = pending_lock_requests.iterate<retry_lock_match_d, retry_lock_match_f>(&match);
    assert(r == 0);

    // retry matching lock requests
    for (size_t i=0; i < match.reqs.get_size(); i++) {
        lock_request *request = match.reqs.fetch_unchecked(i);
        r = request->retry();
    }

    toku_mutex_unlock(&mutex);
    match.reqs.deinit();
}

} /* namespace toku */
