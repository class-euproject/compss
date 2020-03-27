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
PyCOMPSs API - Implement (Versioning)
=====================================
    This file contains the class constraint, needed for the implement
    definition through the decorator.
"""

import inspect
import logging
import os
from functools import wraps
import pycompss.util.context as context
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.util.arguments import check_arguments

if __debug__:
    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {'source_class',
                       'method'}
SUPPORTED_ARGUMENTS = {'source_class',
                       'method'}
DEPRECATED_ARGUMENTS = {'sourceClass'}


class Implement(object):
    """
    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on mpi task creation.
    """

    def __init__(self, *args, **kwargs):
        """
        Store arguments passed to the decorator
        # self = itself.
        # args = not used.
        # kwargs = dictionary with the given implement parameters.

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        self.args = args
        self.kwargs = kwargs
        self.registered = False
        self.first_register = False
        self.scope = context.in_pycompss()
        if self.scope:
            if __debug__:
                logger.debug("Init @implement decorator...")

            # Check the arguments
            check_arguments(MANDATORY_ARGUMENTS,
                            DEPRECATED_ARGUMENTS,
                            SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                            list(kwargs.keys()),
                            "@implement")

    def __call__(self, func):
        """
        Parse and set the implementation parameters within the task core
        element.

        :param func: Function to decorate
        :return: Decorated function.
        """
        @wraps(func)
        def implement_f(*args, **kwargs):
            # This is executed only when called.
            if not self.scope:
                # from pycompss.api.dummy.implement import implement as dummy_implement  # noqa
                # d_i = dummy_implement(self.args, self.kwargs)
                # return d_i.__call__(func)
                raise Exception(not_in_pycompss("implement"))

            if context.in_master():
                # master code
                mod = inspect.getmodule(func)
                self.module = mod.__name__  # not func.__module__

                if (self.module == '__main__' or
                        self.module == 'pycompss.runtime.launch'):
                    # The module where the function is defined was run as
                    # __main__, so we need to find out the real module name.

                    # path = mod.__file__
                    # dirs = mod.__file__.split(os.sep)
                    # file_name = os.path.splitext(
                    #                 os.path.basename(mod.__file__))[0]

                    # Get the real module name from our launch.py variable
                    path = getattr(mod, "APP_PATH")

                    dirs = path.split(os.path.sep)
                    file_name = os.path.splitext(os.path.basename(path))[0]
                    mod_name = file_name

                    i = len(dirs) - 1
                    while i > 0:
                        new_l = len(path) - (len(dirs[i]) + 1)
                        path = path[0:new_l]
                        if "__init__.py" in os.listdir(path):
                            # directory is a package
                            i -= 1
                            mod_name = dirs[i] + '.' + mod_name
                        else:
                            break
                    self.module = mod_name

                # Include the registering info related to @implement

                # Retrieve the base core_element established at @task decorator
                if not self.registered:
                    self.registered = True
                    from pycompss.api.task import current_core_element as cce
                    # Update the core element information with the @implement
                    # information
                    if 'sourceClass' in self.kwargs:
                        another_class = self.kwargs['sourceClass']
                    else:
                        another_class = self.kwargs['source_class']
                    another_method = self.kwargs['method']
                    ce_signature = another_class + '.' + another_method
                    cce.set_ce_signature(ce_signature)
                    # This is not needed since the arguments are already set
                    # by the task decorator.
                    # implArgs = [another_class, another_method]
                    # cce.set_implTypeArgs(implArgs)
                    cce.set_impl_type("METHOD")
            else:
                # worker code
                pass

            if __debug__:
                logger.debug("Executing implement_f wrapper.")

            # The 'self' for a method function is passed as args[0]
            slf = args[0]

            # Replace and store the attributes
            saved = {}
            for k, v in self.kwargs.items():
                if hasattr(slf, k):
                    saved[k] = getattr(slf, k)
                    setattr(slf, k, v)

            # Call the method
            ret = func(*args, **kwargs)

            # Put things back
            for k, v in saved.items():
                setattr(slf, k, v)

            return ret

        implement_f.__doc__ = func.__doc__

        if context.in_master() and not self.first_register:
            import pycompss.api.task as t
            self.first_register = True
            t.register_only = True
            self.__call__(func)(self)
            t.register_only = False

        return implement_f


# ########################################################################### #
# ################## IMPLEMENT DECORATOR ALTERNATIVE NAME ################### #
# ########################################################################### #

implement = Implement
