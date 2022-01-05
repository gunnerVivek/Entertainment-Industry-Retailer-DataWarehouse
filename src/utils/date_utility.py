#TODO: Handle value errors

from datetime import date, datetime
import pytz


def get_current_date(format='%Y%m%d', time_zone='UTC')->str:
    '''
        This method is used to get the current date in the specifed format.
        If the provided date format is not specified, it defaults to %Y%m%d.
        In case specified format is not supported, it defaults to %Y%m%d.

        Parameters
        ----------------
        format   : Format of the date to be returned.
        time_zone: Time zone of the date to be returned.

        Returns
        ----------------
        date string in the specified format.
    '''
    try:
        return datetime.now(pytz.timezone(time_zone)).date().strftime(format)

    except ValueError:
        # TODO:log
        return datetime.now(pytz.timezone('UTC')).date().strftime("%y%m%d")


def get_current_time(format='%H%M%S', time_zone='UTC')->str:
    '''
        This method is used to get the current time in the specifed format.
        If the provided time format is not specified, it defaults to %H%M%S.
        In case specified format or time zone is not supported, it fallsback to defaults.

        Parameters
        ----------------
        format   : Format of the time to be returned.
        time_zone: Time zone of the time to be returned.

        Returns
        ----------------
        time string in the specified format. 
    '''
    try:
        return datetime.now(pytz.timezone(time_zone)).time().strftime(format)
        
    except ValueError:
        return datetime.now(pytz.timezone('UTC')).time().strftime('%H%M%S')
        

def get_current_date_time(format='%Y%m%d%H%M%S', time_zone='UTC')->str:
    '''
        This method is used to get the current date and time in the specifed format.
        If the provided date format is not specified, it defaults to %Y%m%d.
        In case specified format is not supported, it fallsback to defaults.

        Parameters
        ----------------
        format   : Format of the date and time to be returned.
        time_zone: Time zone of the date and time to be returned.

        Returns
        ----------------
        date and time string in the specified format.
    '''
    try:
        return datetime.now(pytz.timezone(time_zone)).strftime(format)

    except ValueError:
        return datetime.now(pytz.timezone('UTC')).strftime('%Y%m%d%H%M%S')
