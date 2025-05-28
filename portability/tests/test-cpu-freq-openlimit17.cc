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

#include <stdio.h>
#include <fcntl.h>
#include <toku_assert.h>
#include <toku_stdint.h>
#include <toku_os.h>
#include <unistd.h>
#include <vector>

// verify that we can compute processor frequency even when out of file descriptors.

int verbose = 0;

static void run_test(void) {
    uint64_t cpuhz;
    int r = toku_os_get_processor_frequency(&cpuhz);
    assert(r == 0);
    if (verbose) {
	printf("%" PRIu64 "\n", cpuhz);
    }
    assert(cpuhz>100000000);
}

int main(void) {
    run_test();

    // consume all of the unused file descriptors
    // keep track of all of the file descriptors so we can close them before end of test
    // otherwise, the leak sanitizer aborts
    std::vector<int> fds;
    while (1) {
        int fd = open("/dev/null", O_RDONLY);
        if (fd < 0)
            break;
        fds.push_back(fd);
    }

    run_test();

    // close all of the test file descriptors
    for (auto fd : fds) {
        int r = close(fd);
        assert(r == 0);
    }

    return 0;
}
