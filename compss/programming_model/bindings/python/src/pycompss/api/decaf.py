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
PyCOMPSs API - DECAF
====================
    This file contains the class decaf, needed for the @decaf task
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

MANDATORY_ARGUMENTS = {'df_script'}
SUPPORTED_ARGUMENTS = {'computing_nodes',
                       'working_dir',
                       'runner',
                       'df_executor',
                       'df_lib',
                       'df_script'}
DEPRECATED_ARGUMENTS = {'computingNodes',
                        'workingDir',
                        'dfExecutor',
                        'dfLib',
                        'dfScript'}


class Decaf(object):
    """
    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on mpi task creation.
    """

    def __init__(self, *args, **kwargs):
        """
        Store arguments passed to the decorator
        # self = itself.
        # args = not used.
        # kwargs = dictionary with the given constraints.

        :param args: Arguments
        :param kwargs: Keyword arguments
        """

        self.args = args
        self.kwargs = kwargs
        self.registered = False
        self.scope = context.in_pycompss()
        if self.scope:
            if __debug__:
                logger.debug("Init @decaf decorator...")

            # Check the arguments
            check_arguments(MANDATORY_ARGUMENTS,
                            DEPRECATED_ARGUMENTS,
                            SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                            list(kwargs.keys()),
                            "@decaf")

            # Get the computing nodes: This parameter will have to go down
            # until execution when invoked.
            if 'computing_nodes' not in self.kwargs and 'computingNodes' not in self.kwargs:
                self.kwargs['computing_nodes'] = 1
            else:
                if 'computingNodes' in self.kwargs:
                    self.kwargs['computing_nodes'] = self.kwargs.pop('computingNodes')
                computing_nodes = kwargs['computing_nodes']
                if isinstance(computing_nodes, int):
                    self.kwargs['computing_nodes'] = kwargs['computing_nodes']
                elif isinstance(computing_nodes, str) and computing_nodes.strip().startswith('$'):
                    env_var = computing_nodes.strip()[1:]  # Remove $
                    if env_var.startswith('{'):
                        env_var = env_var[1:-1]  # remove brackets
                    self.kwargs['computing_nodes'] = int(os.environ[env_var])
                else:
                    raise Exception("Wrong Computing Nodes value at DECAF decorator.")
            if __debug__:
                logger.debug("This DECAF task will have " + str(self.kwargs['computing_nodes']) + " computing nodes.")
        else:
            pass

    def __call__(self, func):
        """
        Parse and set the decaf parameters within the task core element.

        :param func: Function to decorate
        :return: Decorated function.
        """

        @wraps(func)
        def decaf_f(*args, **kwargs):
            if not self.scope:
                # from pycompss.api.dummy.decaf import decaf as dummy_decaf
                # d_d = dummy_decaf(self.args, self.kwargs)
                # return d_d.__call__(func)
                raise Exception(not_in_pycompss("decaf"))

            if context.in_master():
                # master code
                mod = inspect.getmodule(func)
                self.module = mod.__name__  # not func.__module__

                if (self.module == '__main__' or
                        self.module == 'pycompss.runtime.launch'):
                    # The module where the function is defined was run as __main__,
                    # we need to find out the real module name.

                    # path=mod.__file__
                    # dirs=mod.__file__.split(os.sep)
                    # file_name=os.path.splitext(os.path.basename(mod.__file__))[0]

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

                # Include the registering info related to @decaf

                # Retrieve the base core_element established at @task decorator
                if not self.registered:
                    from pycompss.api.task import current_core_element as core_element
                    self.registered = True
                    # Update the core element information with the mpi information
                    core_element.set_impl_type("DECAF")

                    if 'working_dir' in self.kwargs:
                        working_dir = self.kwargs['working_dir']
                    elif 'workingDir' in self.kwargs:
                        working_dir = self.kwargs['workingDir']
                    else:
                        working_dir = '[unassigned]'  # Empty or '[unassigned]'

                    if 'runner' in self.kwargs:
                        runner = self.kwargs['runner']
                    else:
                        runner = 'mpirun'

                    if 'dfScript' in self.kwargs:
                        df_script = self.kwargs['dfScript']
                    else:
                        df_script = self.kwargs['df_script']

                    if 'df_executor' in self.kwargs:
                        df_executor = self.kwargs['df_executor']
                    elif 'dfExecutor' in self.kwargs:
                        df_executor = self.kwargs['dfExecutor']
                    else:
                        df_executor = '[unassigned]'  # Empty or '[unassigned]'

                    if 'df_lib' in self.kwargs:
                        df_lib = self.kwargs['df_lib']
                    elif 'dfLib' in self.kwargs:
                        df_lib = self.kwargs['dfLib']
                    else:
                        df_lib = '[unassigned]'  # Empty or '[unassigned]'
                    impl_signature = 'DECAF.' + df_script
                    core_element.set_impl_signature(impl_signature)
                    impl_args = [df_script, df_executor, df_lib, working_dir, runner]
                    core_element.set_impl_type_args(impl_args)
            else:
                # worker code
                pass

            # This is executed only when called.
            if __debug__:
                logger.debug("Executing decaf_f wrapper.")

            # Set the computing_nodes variable in kwargs for its usage in @task decorator
            kwargs['computing_nodes'] = self.kwargs['computing_nodes']

            if len(args) > 0:
                # The 'self' for a method function is passed as args[0]
                slf = args[0]

                # Replace and store the attributes
                saved = {}
                for k, v in self.kwargs.items():
                    if hasattr(slf, k):
                        saved[k] = getattr(slf, k)
                        setattr(slf, k, v)

            # Call the method
            import pycompss.api.task as t
            t.prepend_strings = False
            ret = func(*args, **kwargs)
            t.prepend_strings = True

            if len(args) > 0:
                # Put things back
                for k, v in saved.items():
                    setattr(slf, k, v)

            return ret

        decaf_f.__doc__ = func.__doc__
        return decaf_f


# ############################################################################# #
# ##################### DECAF DECORATOR ALTERNATIVE NAME ###################### #
# ############################################################################# #

decaf = Decaf
