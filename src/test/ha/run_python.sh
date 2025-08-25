#!/bin/bash
# run_python.sh - a wrapper script that directly replaces the python command

# find available Python interpreters
PYTHON_CMD=$(command -v python3 2>/dev/null || command -v python 2>/dev/null)

if [ -z "$PYTHON_CMD" ]; then
    echo "error: No Python interpreter found (python3 or python)" >&2
    exit 1
fi

# execute the Python interpreter directly and pass all parameters.
exec $PYTHON_CMD "$@"
