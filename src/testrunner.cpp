#include "eqgj.hpp"
#include "smallgj.hpp"
#include "uneqgj.hpp"
#include "altgj.hpp"
#include "paragj.hpp"

#include "basics.hpp"
#include "aggfuncs.hpp"
#include "util.hpp"
#include "tests.hpp"

#include <iostream>
#include <algorithm>

using namespace parajoin;

int parajoin::prt_size;
int parajoin::num_threads;

int main()
{
    // initialize randomizer
    const uint64_t seed = time(0);
    srand(seed);

    // Variables
    parajoin::prt_size = 1e1;
    parajoin::num_threads = 20;
    uint l_size = 1e3;
    uint r_size = 1e3;
    uint sel_fac = 1e3;

    std::cout << "Running tests for GroupJoin" << std::endl;
    std::cout << "Input seed: " << seed << std::endl;

    std::cout << "Running tests for =-groupjoin.." << std::endl;
    testEqGJ(l_size, r_size, sel_fac);

    std::cout << "Running tests for unique =-groupjoin.." << std::endl;
    testUniqueEqGJ(l_size, r_size, sel_fac);

    std::cout << "Running tests for !=-groupjoin.." << std::endl;
    testUneqGJ(l_size, r_size, sel_fac);

    std::cout << "Running tests for <-groupjoin.." << std::endl;
    testSmallGJ(l_size, r_size, sel_fac);

    std::cout << "All tests have been successfully passed!" << std::endl;
}
