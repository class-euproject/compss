#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import unittest
import logging

#
# Logger definition
#

logger = logging.getLogger(__name__)


#
# Code Reuser class
#

class CodeReuser(object):
    """
    Creates an object to reuse the previously generated parallel code of the given function

    Attributes:
            - func : Function to be parallelized
            - force_autogen : Flag to force the autogeneration even if the files already exist
            - original_file : File containing the original function code
            - bkp_file : Backup of the original file
            - autogen_file : File containing the new parallel function code
    """

    def __init__(self, func=None, force_autogen=True):
        """
        Creates a CodeReuser object for the given function

        :param func: Function to be parallelized
        :param force_autogen: Flag to force the autogeneration even if the files already exist
        """

        self.func = func
        self.force_autogen = force_autogen

        # Retrieve original file
        import inspect
        try:
            self.original_file = inspect.getfile(func)
        except Exception as e:
            raise CodeReuserException("[ERROR] Cannot find original file", e)

        # Set backup file
        import os
        file_name = os.path.splitext(self.original_file)[0]
        self.bkp_file = file_name + "_bkp.py"
        # Set new file
        self.autogen_file = file_name + "_autogen.py"

    def can_reuse(self):
        """
        Returns whether the autogenerated files can be reused or not

        :return: True if the autogenerated files exist and can be reused. False otherwise.
        """
        import os
        autogen_file_exists = os.path.isfile(self.autogen_file)

        return not self.force_autogen and autogen_file_exists

    def reuse(self):
        """
        Replaces the func code by the code previously stored in the autogen file

        :return: Pointer to the new function
        :raise CodeReuserException:
        """

        if __debug__:
            logger.debug(
                "[code_reuser] Reusing code of " + str(self.func) + " by code in file " + str(self.autogen_file))

        # Wrap the internal replace method to catch exceptions and restore user code
        try:
            new_func = self._reuse()
        except Exception as e:
            self.restore()
            raise CodeReuserException("[ERROR] Cannot replace func " + str(self.func), e)

        # Finish
        if __debug__:
            logger.debug("[code_reuser] New function: " + str(new_func))
        return new_func

    def _reuse(self):
        """
        Internal method to replace the func code by the code previously stored in the autogen file

        :return: Poiunter to the new function
        :raise CodeReuserException:
        """

        # Backup user file
        try:
            from shutil import copyfile
            copyfile(self.original_file, self.bkp_file)
        except Exception as e:
            raise CodeReuserException("[ERROR] Cannot backup source file", e)
        if __debug__:
            logger.debug("[code_reuser] User code backup in file " + str(self.bkp_file))

        # Copy autogen content to original file
        try:
            from shutil import copyfile
            copyfile(self.autogen_file, self.original_file)
        except Exception as e:
            raise CodeReuserException("[ERROR] Cannot replace original file", e)

        # Load new function from new file
        # Similar to: from new_module import func.__name__ as new_func
        import os
        new_module = os.path.splitext(os.path.basename(self.original_file))[0]
        if __debug__:
            logger.debug("[code_reuser] Import module " + str(self.func.__name__) + " from " + str(new_module))
        try:
            import importlib
            new_func = getattr(importlib.import_module(new_module), self.func.__name__)
        except Exception as e:
            raise CodeReuserException(
                "[ERROR] Cannot load new function and module " + str(self.func.__name__) + " from " + str(new_module),
                e)

        # Return the new function
        return new_func

    def restore(self):
        """
        Restores the user files
        """

        if __debug__:
            logger.debug("[code_reuser] Restoring user code")

        import os
        if os.path.isfile(self.bkp_file):
            # Restore user file
            from shutil import copyfile
            copyfile(self.bkp_file, self.original_file)

            # Clean intermediate files
            os.remove(self.bkp_file)


#
# Exception Class
#

class CodeReuserException(Exception):

    def __init__(self, msg=None, nested_exception=None):
        self.msg = msg
        self.nested_exception = nested_exception

    def __str__(self):
        s = "Exception on CodeReuser class.\n"
        if self.msg is not None:
            s = s + "Message: " + str(self.msg) + "\n"
        if self.nested_exception is not None:
            s = s + "Nested Exception: " + str(self.nested_exception) + "\n"
        return s


#
# UNIT TEST CASES
#

class TestCodeReuser(unittest.TestCase):

    def test_can_reuse(self):
        # Insert function file into PYTHONPATH
        import os
        dir_path = os.path.dirname(os.path.realpath(__file__))
        tests_path = dir_path + "/tests"
        import sys
        sys.path.insert(0, tests_path)

        # Import function to replace
        from tests.original import test_func as f

        # Check if can reuse
        cr = CodeReuser(f, force_autogen=False)
        can_reuse = cr.can_reuse()

        # Check result
        self.assertTrue(can_reuse)

    def test_reuse(self):
        # Insert function file into PYTHONPATH
        import os
        dir_path = os.path.dirname(os.path.realpath(__file__))
        tests_path = dir_path + "/tests"
        import sys
        sys.path.insert(0, tests_path)

        # Import function to replace
        from tests.original import test_func as f
        import inspect
        user_file = inspect.getfile(f)

        # Reuse
        try:
            # Perform reuse
            cr = CodeReuser(f)
            new_func = cr.reuse()

            # Check function has been reloaded
            self.assertNotEqual(f, new_func)

            # Check final user file content
            expected_file = tests_path + "/expected.python"
            with open(expected_file, 'r') as f:
                expected_content = f.read()
            with open(user_file, 'r') as f:
                user_content = f.read()
            self.assertEqual(user_content, expected_content)
        except Exception:
            raise
        finally:
            # Clean intermediate files
            if cr is not None:
                cr.restore()


#
# MAIN FOR UNIT TEST
#

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(levelname)s | %(name)s - %(message)s')
    unittest.main()
