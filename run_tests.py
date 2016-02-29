#!/usr/bin/python
#
# Scripts that help set up all proper paths for
# unittests and then runs the tests. Adapted from:
# https://cloud.google.com/appengine/docs/python/tools/localunittesting
#
import optparse
import sys
import unittest

USAGE = """%prog SDK_PATH
Run unit tests for App Engine apps.

SDK_PATH    Path to the SDK installation"""


def main(sdk_path):
    sys.path.insert(0, sdk_path)
    import api_server
    api_server.fix_sys_path()
    suite = unittest.loader.TestLoader().\
        discover('.', pattern='*_test.py')
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        sys.exit(1)

if __name__ == '__main__':
    parser = optparse.OptionParser(USAGE)
    options, args = parser.parse_args()
    if len(args) != 1:
        print 'Error: Exactly 1 arguments required.'
        parser.print_help()
        sys.exit(1)
    SDK_PATH = args[0]
    main(SDK_PATH)
