# Databricks notebook source
# DBTITLE 1,testcase_execution
import sys
import os
import pytest
from pyspark.sql.functions import *

project_root = "/Workspace/Users/deep.kothari94@gmail.com/PEI_Ecommerce"
sys.path.insert(0, project_root)

print("RUNNING TESTS....")

result = pytest.main([
    os.path.join(project_root, "tests", "test_cases.py"),
    "-v",
    "--tb=short",
    "--assert=plain",          
    "-p", "no:cacheprovider"
])

print("TESTCASE EXECUTION COMPLETED\nResult code: {}".format(result))

if result != 0:
    raise Exception("TEST CASE GOT FAILED")

print("ALL TESTCASES PASSED")