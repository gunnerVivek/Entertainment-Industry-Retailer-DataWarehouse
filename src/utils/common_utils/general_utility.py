
# https://stackoverflow.com/questions/1952464/in-python-how-do-i-determine-if-an-object-is-iterable
def is_iterable(item, str_ok=False):
    '''
        Checks if an object is iterable.
    '''
    try:
        iter(item)
    except TypeError as e:
        return False
    else:# check for str condition
        # if string should be considered as iterable
        if str_ok and isinstance(item, str):
            return True

        # if string should not be considered as iterable
        elif not str_ok and isinstance(item, str):
            return False
        
        # Not string and iter() did not raise exception
        else:
            return True

