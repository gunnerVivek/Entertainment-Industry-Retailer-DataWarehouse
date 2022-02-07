import inspect


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


def get_caller_module():
    '''
    Returns Full Module path of the caller. Caller here refers to immediate 
    preceding caller.
    A --> get_caller_module() : returns A
    A -- > B --> get_caller_module() : returns A  
    '''

    return inspect.stack()[1].filename


def get_caller_class():
    '''
    Returns function name of the caller if available. Caller here refers to immediate 
    preceding caller.
    '''
    try:
        return inspect.stack()[1][0].f_locals["self"].__class__.__name__ or "<class_name>"
    except KeyError as e: # when not called from class
        return "<class_name>"


def get_caller_function():
    '''
    Returns function name of the caller if available. Caller here refers to immediate 
    preceding caller.
    A --> get_caller_function() : returns A
    A -- > B --> get_caller_functions() : returns A  
    '''

    try:
        return inspect.stack()[1].function or "<function_name>"
    except KeyError as e: # when not called from within a function
        return "<function_name>"


def construct_full_calling_name(*args):

    sep = " >> "
    args = [str(x) for x in args]

    return sep.join(args)

