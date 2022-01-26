
from unittest import TestCase as tc
from src.utils import common_utility


dummy_object = tc()


def test_list_is_iterable():
    '''
    This test cases checks for list to be returned as iterable
    '''
    ls = [1, 'abc', 2.5, 'd']
    tc.assertTrue(dummy_object, common_utility.is_iterable(item=ls))


def test_string_not_iterable():
    '''
    This test case checks for string type to be returned as not iterable.
    '''    
    string_obj = 'test_case'
    tc.assertFalse(dummy_object, common_utility.is_iterable(item=string_obj))


def test_string_is_iterable():
    '''
    This test case checks for string type to be returned as iterable.
    '''
    string_obj = 'test_case'
    tc.assertTrue(dummy_object, common_utility.is_iterable(item=string_obj, str_ok=True))
