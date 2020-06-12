import sys
import unittest
import json
import argparse


class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print("setupClass")

    @classmethod
    def tearDownClass(cls):
        print("tearDownClass")

    def setUp(self):
        print("setUp")

    def tearDown(self):
        print("tearDown")

    def test_method1(self):
        print("method1")
        assert True

    def test_method2(self):
        print("methond2")
        assert True

    def another_method(self):
        c = MyTestCase()
        c.test_method1()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="dev.json")
    parser.add_argument('unittest_args', nargs='*')
    args = parser.parse_args()
    j = json.load(open(args.config, 'r'))
    print(j["redis_server"])
    sys.argv[:1] = args.unittest_args

    unittest.main()
