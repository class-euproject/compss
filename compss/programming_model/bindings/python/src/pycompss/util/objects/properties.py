#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# -*- coding: utf-8 -*-

"""
PyCOMPSs Util - Object properties
=================================
    Offers some functions that check properties about objects.
    For example, check if an object belongs to a module and so on.
"""

import os, sys
import imp
import inspect
from pycompss.runtime.commons import IS_PYTHON3

if IS_PYTHON3:
    import builtins as _builtins
else:
    import __builtin__ as _builtins

def get_defining_class(meth):
    """
    Given a method

    :param meth: Method to check its defining class
    :return: Class which meth belongs. None if not found.
    """
    if inspect.ismethod(meth):
        for cls in inspect.getmro(meth.__self__.__class__):
            if cls.__dict__.get(meth.__name__) is meth:
                return cls
    if inspect.isfunction(meth):
        return getattr(inspect.getmodule(meth),
                       meth.__qualname__.split('.<locals>',
                                               1)[0].rsplit('.', 1)[0])
    # Return not required since None would have been implicitly returned anyway
    return None


def get_module_name(path, file_name):
    """
    Get the module name considering its path and filename.

    Example: runcompss -d src/kmeans.py
             path = "test/kmeans.py"
             file_name = "kmeans" (without py extension)
             return mod_name = "test.kmeans"

    :param path: relative path until the file.py from where the runcompss has
                 been executed
    :param file_name: python file to be executed name
                      (without the py extension)
    :return: the module name
    """
    dirs = path.split(os.path.sep)
    mod_name = file_name
    i = len(dirs) - 1
    while i > 0:
        new_l = len(path) - (len(dirs[i]) + 1)
        path = path[0:new_l]
        if '__init__.py' in os.listdir(path):
            # directory is a package
            i -= 1
            mod_name = dirs[i] + '.' + mod_name
        else:
            break
    return mod_name


def get_top_decorator(code, decorator_keys):
    """
    Retrieves the decorator which is on top of the current task decorators
    stack.

    :param code: Tuple which contains the task code to analyse and the number
                 of lines of the code.
    :param decorator_keys: Typle which contains the available decorator keys
    :return: the decorator name in the form "pycompss.api.__name__"
    """
    # Code has two fields:
    # code[0] = the entire function code.
    # code[1] = the number of lines of the function code.
    func_code = code[0]
    decorators = [l.strip() for l in func_code if l.strip().startswith('@')]
    # Could be improved if it stops when the first line without @ is found,
    # but we have to be care if a decorator is commented (# before @)
    # The strip is due to the spaces that appear before functions definitions,
    # such as class methods.
    for dk in decorator_keys:
        for d in decorators:
            if d.startswith('@' + dk):
                return 'pycompss.api.' + dk.lower()  # each decorator __name__
    # If no decorator is found, the current decorator is the one to register
    return __name__


def get_wrapped_source(f):
    """
    Gets the text of the source code for the given function.

    :param f: Input function
    :return: Source
    """

    if hasattr(f, "__wrapped__"):
        # has __wrapped__, going deep
        return get_wrapped_source(f.__wrapped__)
    else:
        # Returning getsource
        try:
            source = inspect.getsource(f)
        except TypeError:
            # This is a numba jit declared task
            source = inspect.getsource(f.py_func)
        return source


def get_wrapped_sourcelines(f):
    """
    Gets a list of source lines and starting line number for the given function

    :param f: Input function
    :return: Source lines
    """
    if hasattr(f, '__wrapped__'):
        # has __wrapped__, apply the same function to the wrapped content
        return _get_wrapped_sourcelines(f.__wrapped__)
    else:
        # Returning getsourcelines
        try:
            sourcelines = inspect.getsourcelines(f)
        except TypeError:
            # This is a numba jit declared task
            sourcelines = inspect.getsourcelines(f.py_func)
        return sourcelines


def _get_wrapped_sourcelines(f):
    """
    [PRIVATE] Recursive function which gets a list of source lines and starting
    line number for the given function.

    :param f: Input function
    :return: Source lines
    """
    if hasattr(f, "__wrapped__"):
        # has __wrapped__, going deep
        return _get_wrapped_sourcelines(f.__wrapped__)
    else:
        # Returning getsourcelines
        try:
            sourcelines = inspect.getsourcelines(f)
        except TypeError:
            # This is a numba jit declared task
            sourcelines = inspect.getsourcelines(f.py_func)
        return sourcelines


def is_module_available(module_name):
    """
    Checks if a module is available in the current Python installation.

    :param module_name: Name of the module
    :return: Boolean -> True if the module is available, False otherwise
    """
    try:
        imp.find_module(module_name)
        return True
    except ImportError:
        return False


def is_basic_iterable(obj):
    """
    Checks if an object is a basic iterable. By basic iterable we want to
    mean objects that are iterable and from a basic type.

    :param obj: Object to be analysed
    :return: Boolean -> True if obj is a basic iterable (see list below),
                        False otherwise
    """
    return isinstance(obj, (list, tuple, bytearray, set, frozenset, dict))


def object_belongs_to_module(obj, module_name):
    """
    Checks if a given object belongs to a given module (or some sub-module).

    :param obj: Object to be analysed
    :param module_name: Name of the module we want to check
    :return: Boolean -> True if obj belongs to the given module,
                        False otherwise
    """
    return any(module_name == x for x in type(obj).__module__.split('.'))


def create_object_by_con_type(con_type, default=object):
    """
    Knowing its class name create an 'empty' object.
    :param con_type: object type info in <path_to_module>:<class_name> format.
    :param default: default object type to be returned if class not found.
    :return: 'empty' object of a type
    """
    path, class_name = con_type.split(":")
    if hasattr(_builtins, class_name):
        _obj = getattr(_builtins, class_name)
        return _obj()

    # try:
    directory, module_name = os.path.split(path)
    module_name = os.path.splitext(module_name)[0]

    klass = globals().get(class_name, None)
    if klass:
        return klass()

    if module_name not in sys.modules:
        sys.path.append(directory)
        module = __import__(module_name)
        sys.modules[module_name] = module
    else:
        module = sys.modules[module_name]

    klass = getattr(module, class_name)
    ret = klass()
    return ret
    # except Exception:
    #     # todo: handle the exception?
    #     pass
    # return default()
