from unittest import TestCase as tc
from datetime import datetime
import pytz

# udf
from src.utils import date_utility

# dummy object
# this object is needed to use unittest assert Methods
# hangover form XUnit frameworks
# https://stackoverflow.com/a/56254695/3072095
dummy_object = tc()


def test_current_date_utc_default():

    today = datetime.now(pytz.timezone('UTC')).date().strftime("%y%m%d")
    tc.assertEqual(dummy_object, date_utility.get_current_date(), today)


def test_current_date_utc():

    today = datetime.now(pytz.timezone('UTC')).date().strftime("%y%m%d")
    tc.assertEqual(dummy_object, date_utility.get_current_date(
        '%Y%m%d', 'UTC'), today)


def test_current_date_ist():

    today = datetime.now(pytz.timezone('Asia/Kolkata')).date().strftime("%y%m%d")
    tc.assertEqual(dummy_object, date_utility.get_current_date('%Y%m%d', 'Asia/Kolkata')
                    , today)


def test_current_date_utc_format_error():

    # should not raise an error and return default values
    today = datetime.now(pytz.timezone('UTC')).date().strftime("%y%m%d")
    tc.assertEqual(dummy_object, date_utility.get_current_date('%q'), today)
    

# -- current time --

def test_current_time_utc_default():

    # today = datetime.now(pytz.timezone('UTC')).date().strftime("%y%m%d")
    now = datetime.now(pytz.timezone('UTC')).time().strftime("%H%M%S")
    tc.assertEqual(dummy_object, date_utility.get_current_time(), now)


def test_current_time_utc():

    now = datetime.now(pytz.timezone('UTC')).time().strftime("%H%M%S")
    tc.assertEqual(dummy_object, date_utility.get_current_date('%H%M%S', 'UTC'), now)


def test_current_time_ist():

    # today = datetime.now(pytz.timezone('Asia/Kolkata')).date().strftime("%y%m%d")
    now = datetime.now(pytz.timezone('Asia/Kolkata')).time().strftime("%H%M%S")
    tc.assertEqual(dummy_object, date_utility.get_current_time('%H%M%S', 'Asia/Kolkata'), now)


def test_current_time_utc_format_error():

    # should not raise an error and return default values
    now = datetime.now(pytz.timezone('UTC')).time().strftime("%H%M%S")
    tc.assertEqual(dummy_object, date_utility.get_current_time('%q'), now)
