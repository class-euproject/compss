#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs API - COMPSs
==================
    This file contains the class COMPSs, needed for the compss
    definition through the decorator.
"""

import inspect
import logging
import os
import pycompss.util.context as context

if __debug__:
    logger = logging.getLogger(__name__)


class COMPSs(object):
    """
    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on compss task creation.
    """

    def __init__(self, *args, **kwargs):
        """
        Store arguments passed to the decorator
        # self = itself.
        # args = not used.
        # kwargs = dictionary with the given COMPSs parameters

        :param args: Arguments
        :param kwargs: Keyword arguments
        """

        self.args = args
        self.kwargs = kwargs
        self.registered = False
        self.scope = context.in_pycompss()
        if self.scope:
            if __debug__:
                logger.debug("Init @compss decorator...")

            # Get the computing nodes: This parameter will have to go down until
            # execution when invoked.
            if 'computingNodes' not in self.kwargs:
                self.kwargs['computingNodes'] = 1
            else:
                computing_nodes = self.kwargs['computingNodes']
                if isinstance(computing_nodes, int):
                    # Nothing to do
                    pass
                elif isinstance(computing_nodes, str):
                    # Check if it is an environment variable to be loaded
                    if computing_nodes.strip().startswith('$'):
                        # Computing nodes is an ENV variable, load it
                        env_var = computing_nodes.strip()[1:]  # Remove $
                        if env_var.startswith('{'):
                            env_var = env_var[1:-1]  # remove brackets
                        try:
                            self.kwargs['computingNodes'] = int(os.environ[env_var])
                        except ValueError:
                            raise Exception("ERROR: ComputingNodes value cannot be cast from ENV variable to int")
                    else:
                        # ComputingNodes is in string form, cast it
                        try:
                            self.kwargs['computingNodes'] = int(computing_nodes)
                        except ValueError:
                            raise Exception("ERROR: ComputingNodes value cannot be cast from string to int")
                else:
                    raise Exception("ERROR: Wrong Computing Nodes value at COMPSs decorator.")
            if __debug__:
                logger.debug("This COMPSs task will have " + str(self.kwargs['computingNodes']) + " computing nodes.")
        else:
            pass

    def __call__(self, func):
        """
        Parse and set the compss parameters within the task core element.

        :param func: Function to decorate
        :return: Decorated function.
        """

        def compss_f(*args, **kwargs):
            if not self.scope:
                # from pycompss.api.dummy.compss import COMPSs as dummy_compss
                # d_m = dummy_compss(self.args, self.kwargs)
                # return d_m.__call__(func)
                raise Exception("The compss decorator only works within PyCOMPSs framework.")

            if context.in_master():
                # master code
                mod = inspect.getmodule(func)
                self.module = mod.__name__  # not func.__module__

                if self.module == '__main__' or self.module == 'pycompss.runtime.launch':
                    # The module where the function is defined was run as __main__,
                    # we need to find out the real module name.

                    # Get the real module name from our launch.py variable
                    path = getattr(mod, "app_path")
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

                # Include the registering info related to @compss

                # Retrieve the base core_element established at @task decorator
                from pycompss.api.task import current_core_element as core_element
                if not self.registered:
                    self.registered = True
                    # Update the core element information with the compss information
                    core_element.set_impl_type("COMPSs")

                    if 'runcompss' in self.kwargs:
                        runcompss = self.kwargs['runcompss']
                    else:
                        runcompss = '[unassigned]'  # Empty or '[unassigned]'

                    if 'flags' in self.kwargs:
                        flags = self.kwargs['flags']
                    else:
                        flags = '[unassigned]'  # Empty or '[unassigned]'

                    app_name = self.kwargs['app_name']

                    if 'workingDir' in self.kwargs:
                        working_dir = self.kwargs['workingDir']
                    else:
                        working_dir = '[unassigned]'  # Empty or '[unassigned]'

                    impl_signature = 'COMPSs.' + app_name
                    core_element.set_impl_signature(impl_signature)
                    impl_args = [runcompss, flags, app_name, working_dir]
                    core_element.set_impl_type_args(impl_args)
            else:
                # worker code
                pass

            # This is executed only when called.
            if __debug__:
                logger.debug("Executing compss_f wrapper.")

            # Set the computingNodes variable in kwargs for its usage
            # in @task decorator
            kwargs['computingNodes'] = self.kwargs['computingNodes']

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

        compss_f.__doc__ = func.__doc__
        return compss_f


# ############################################################################# #
# ###################### COMPSs DECORATOR ALTERNATIVE NAME ####################### #
# ############################################################################# #

compss = COMPSs
