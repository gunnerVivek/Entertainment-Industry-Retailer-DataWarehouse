from datetime import date


def get_current_date(format='%Y%m%d')->str:
    '''
        This method is used to get the current date in the specifed format.
        If the provided date format is not specified, it defaults to %Y%m%d.
        In case specified format is not supported, it defaults to %Y%m%d.

        Parameters
        ----------------
        format: Format of the date to be returned.

        Returns
        ----------------
        date string in the specified format.
    '''
    try:
        return date.today().strftime(format)
    except ValueError:
        # TODO:log
        ''
    finally:
        return date.today().strftime("%Y%m%d")


def get_current_time(format='%H%M%S')->str:
    '''
        This method is used to get the current time in the specifed format.
    '''
    try:
        return date.today().strftime(format)
    except ValueError:
        ''
    finally:
        return date.today().strftime("%H%M%S")


def get_current_date_time(format='%Y%m%d%H%M%S')->str:
    '''
        This method is used to get the current date and time in the specifed format.
    '''

    try:
        return date.today().strftime(format)
    except ValueError:
        ''
    finally:
        return date.today().strftime("%Y%m%d%H%M%S")


# if __name__ == '__main__':
#     print(get_current_date('%q'))
#     # print(get_current_time())
#     # print(get_current_date_time())
