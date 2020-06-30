# -*- coding:utf-8 -*-
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms
# and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
# WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# ----------------------------------------------------------------------------
"""
@brief      The command line parser module.
@details    Parse command line parameters and store them as variables with
            the same name.
"""

# export module
__all__ = ["CommandLineParser", "Option"]

# system import.
import sys as _sys
import optparse as _optparse

# import typing for comment.
try:
    from typing import Dict
    from typing import List
    from typing import Tuple
    from typing import Any
except ImportError:
    Dict = dict
    List = list
    Tuple = tuple
    Any = str or int or complex or list or dict

# local import
from gspylib.common.ErrorCode import ErrorCode


class Option(_optparse.Option, object):
    """
    The command line option class which use to the "OptionParser" instance.
    But this class does not accept the "dest"
    parameter.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the command line option instance.

        :param args:            The command line command string, maximum
                                length is 2.
        :param kwargs:          The command line command parameters.
        :param action:          The named parameter, specified command line
                                parameter action allowed by option parser.
        :param type:            The named parameter, specified command line
                                parameter type of current option.
        :param default:         The named parameter, specified command line
                                parameter default value.
        :param nargs:           The named parameter, specified the number of
                                the command line parameter value.
        :param const:           The named parameter, specified the const
                                value of the command line parameter.
        :param choices:         The named parameter, specified the choice
                                range of the command line parameter, the item
                                 in the choices list must be string type,
                                 and must not set the "type" parameter.
                                 Otherwise, it will lead to unexpected errors.
        :param callback:        The named parameter, specified the handler
                                function for the command line parameter.
        :param callback_args:   The named parameter, specified the unnamed
                                parameters of the handler function for the
                                 command line parameter.
        :param callback_kwargs: The named parameter, specified the named
                                parameters of the handler function for the
                                 command line parameter.
        :param help:            The named parameter, the help string for the
                                command line parameter.
        :param metavar:         The named parameter, the display string for
                                the command line parameter value.

        :type args:             str
        :type kwargs:           *
        :type action:           str
        :type type:             str
        :type default:          *
        :type nargs:            int
        :type const:            *
        :type choices:          List[str]
        :type callback:         function
        :type callback_args:    tuple
        :type callback_kwargs:  dict
        :type help:             str
        :type metavar:          str
        """
        # Remove the "dest" parameter.
        if "dest" in kwargs:
            kwargs.pop("dest")
        # Initialize the command line option instance.
        _optparse.Option.__init__(self, *args, **kwargs)


class OptionParser(_optparse.OptionParser, object):
    """
    The command line option parser.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the internal command line parser.

        :param args:                The additional unnamed parameter of the
                                    command line parser.
        :param kwargs:              The additional named parameter of the
                                    command line parser.
        :param usage:               A usage string for your program.  Before
                                    it is displayed to the user, "%prog" will
                                     be expanded to the name of your program
                                     (prog or os.path.basename(sys.argv[0])).
        :param option_list:         A list of option instance for this parser.
        :param option_class:        The command line option class type,
                                    default is "optparse.Option".
        :param version:             The version string for this scripts.
        :param conflict_handler:    The solutions after command line options
                                    conflict, "resolve" will override before
                                     option, "errors" will raise error,
                                     default is "error".
        :param description:         A paragraph of text giving a brief
                                  overview of your program. optparse re-formats
                                     this paragraph to fit the current
                                     terminal width and prints it when the user
                                     requests help (after usage, but before
                                     the list of options).
        :param formatter:           The formatter instance for the
                                    description information.
        :param add_help_option:     Whether add the help option instance
                                    automatically.
        :param prog:                The name of the current program (to
                                    override os.path.basename(sys.argv[0])).
        :param epilog:              A paragraph of help text to print after
        option help.

        :type args:                 str | list | bool | type |
                                    _optparse.IndentedHelpFormatter
        :type kwargs:               str | list | bool | type |
                                    _optparse.IndentedHelpFormatter
        :type usage:                str
        :type option_list:          List[Option]
        :type option_class:         type
        :type version:              str
        :type conflict_handler:     str
        :type description:          str
        :type formatter:            _optparse.IndentedHelpFormatter
        :type add_help_option:      bool
        :type prog:                 str
        :type epilog:               str
        """
        # Call the parent init function.
        _optparse.OptionParser.__init__(self, *args, **kwargs)

    def print_help(self, _file=_sys.stderr):
        """
        print_help(file : file = stderr)

        Print an extended help message, listing all options and any help
        text provided with them, to 'file' (default
         stderr).

        :param _file:   The file descriptor instance.
        :type _file:    file

        :rtype: None
        """
        _optparse.OptionParser.print_help(self, _file)

    def print_usage(self, _file=_sys.stderr):
        """
        print_usage(file : file = stderr)

        Print the usage message for the current program (self.usage) to
        'file' (default stderr).  Any occurrence of the
         string "%prog" in self.usage is replaced with the name of the
         current program (basename of sys.argv[0]).
         Does nothing if self.usage is empty or not defined.

        :param _file:   The file descriptor instance.
        :type _file:    file

        :rtype: None
        """
        _optparse.OptionParser.print_usage(self, _file)

    def print_version(self, _file=_sys.stderr):
        """
        print_version(file : file = stderr)

        Print the version message for this program (self.version) to 'file'
        (default stderr).  As with print_usage(),
         any occurrence of "%prog" in self.version is replaced by the
         current program's name.  Does nothing if
         self.version is empty or undefined.

        :param _file:   The file descriptor instance.
        :type _file:    file

        :rtype: None
        """
        _optparse.OptionParser.print_version(self, _file)

    def error(self, _msg):
        """
        error(msg : string)

        Print a usage message incorporating 'msg' to stderr and exit. If you
        override this in a subclass, it should not
         return -- it should either exit or raise an exception.

        :param _msg:    The error message.
        :type _msg:     str

        :rtype: None
        """
        raise Exception(ErrorCode.GAUSS_500["GAUSS_50015"] % _msg)


class CommandLineMetaClass(type):
    """
    The command line parser metaclass.

    Used to magically save command line parsing options instances.
    """

    def __new__(mcs, name, bases, attrs):
        """
        Create an new command line parser class.

        :param name:    The name of the current class.
        :param bases:   The parent instances of the current class.
        :param attrs:   The attribute dict of the current class.

        :type name:     str
        :type bases:    Tuple[type]
        :type attrs:    Dict[str, Any]
        :return:
        """
        # If it is the base command line parser class, we will do nothing.
        if name == "CommandLineOption":
            return type.__new__(mcs, name, bases, attrs)

        # Store the command line option instance mapping.
        mappings = {}
        # Store the attribute key-value pair list of the current class.
        items = list(attrs.items())

        # Store the command line option instance to the mapping, and remove
        # it from current class attribute.
        for key, value in items:
            if isinstance(value, Option):
                mappings.setdefault(key, value)
                attrs.pop(key)

                # Add the additional function.
                if value.action in ["append", "append_const", "count"]:
                    def ensure_value(_self, _attr, _value):
                        """
                        Ensure the non-existence of object attributes and
                        set the value of attributes.

                        :param _self:   The object instance.
                        :param _attr:   The object attribute name.
                        :param _value:  The object attribute value.

                        :type _self:    Option
                        :type _attr:    str
                        :type _value:   *

                        :return:    Return the object attribute value.
                        :rtype:     *
                        """
                        if not hasattr(_self, _attr) or getattr(_self,
                                                                _attr) is None:
                            setattr(_self, _attr, _value)
                        return getattr(_self, _attr)

                    # Add function.
                    attrs["ensure_value"] = ensure_value

        # Store the mapping into a named parameter of current class.
        attrs["__mappings__"] = mappings

        return type.__new__(mcs, name, bases, attrs)


class CommandLineParser(object):
    """
    The base class of the command line parser.
    """
    # Set the metaclass type, this approach is not supported python 3.x.
    __metaclass__ = CommandLineMetaClass

    def __init__(self, _parameters=None, *args, **kwargs):
        """
        Initialize the command line parser.

        :param _parameters:         The command line parameters list,
                                    default is sys.argv.
        :param args:                The additional unnamed parameter of the
                                    command line parser.
        :param kwargs:              The additional named parameter of the
                                    command line parser.
        :param usage:               A usage string for your program.  Before
                                    it is displayed to the user, "%prog" will
                                     be expanded to the name of your program
                                     (prog or os.path.basename(sys.argv[0])).
        :param option_list:         A list of option instance for this parser.
        :param option_class:        The command line option class type,
                                    default is "optparse.Option".
        :param version:             The version string for this scripts.
        :param conflict_handler:    The solutions after command line options
                                    conflict, "resolve" will override before
                                     option, "errors" will raise error,
                                     default is "error".
        :param description:         A paragraph of text giving a brief
                                  overview of your program. optparse re-formats
                                     this paragraph to fit the current
                                     terminal width and prints it when the user
                                     requests help (after usage, but before
                                     the list of options).
        :param formatter:           The formatter instance for the
                                    description information.
        :param add_help_option:     Whether add the help option instance
                                    automatically.
        :param prog:                The name of the current program (to
                                    override os.path.basename(sys.argv[0])).
        :param epilog:              A paragraph of help text to print after
                                    option help.

        :type _parameters:          List[str] | None
        :type args:                 str | list | bool | type |
                                    _optparse.IndentedHelpFormatter
        :type kwargs:               str | list | bool | type |
                                    _optparse.IndentedHelpFormatter
        :type usage:                str
        :type option_list:          List[Option]
        :type option_class:         type
        :type version:              str
        :type conflict_handler:     str
        :type description:          str
        :type formatter:            _optparse.IndentedHelpFormatter
        :type add_help_option:      bool
        :type prog:                 str
        :type epilog:               str
        """
        # Create a new command line parser.
        opt = OptionParser(*args, **kwargs)

        # Add the "dest" attribute to the command line option instance,
        # and add the option instance to the parser.
        # noinspection PyUnresolvedReferences
        for key, value in list(self.__mappings__.items()):
            setattr(value, "dest", key)
            opt.add_option(value)

        # Parse the command line parameter.
        if not _parameters:
            _parameters = _sys.argv[1:]
        _, unknown_args = opt.parse_args(_parameters, self)

        # If some command line parameter does not supplied by user, we will
        # set it to "None".
        # noinspection PyUnresolvedReferences
        for key in list(self.__mappings__.keys()):
            if not hasattr(self, key):
                # noinspection PyUnresolvedReferences
                value = self.__mappings__.get(key)
                if hasattr(value, "default") and getattr(value,
                                                         "default") != \
                        _optparse.NO_DEFAULT:
                    setattr(self, key, getattr(value, "default"))
                elif hasattr(value, "const") and getattr(value,
                                                         "const") != \
                        _optparse.NO_DEFAULT:
                    setattr(self, key, getattr(value, "const"))
                else:
                    setattr(self, key, None)

        # If it contains configuration that cannot be resolved, save it.
        if unknown_args:
            setattr(self, "unknown_args", unknown_args)


class ExecuteCommand(object):
    """

    """

    def __init__(self):
        """

        """
        pass
