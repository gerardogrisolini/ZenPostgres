import XCTest

import ZenPostgresTests

var tests = [XCTestCaseEntry]()
tests += ZenPostgresTests.allTests()
XCTMain(tests)
