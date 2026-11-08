#!/usr/bin/env python3
"""Compile the actual gs_find_memory_data body with deterministic load hooks.

No server is required. Run with --git-ref HEAD to demonstrate the old failure
before committing the fix. This tests protocol interleavings, not CPU barriers;
the native non-stop debugger check separately verifies the built Release path.
"""
import argparse
from pathlib import Path
import subprocess
import tempfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
SOURCE = 'src/gausskernel/cbb/communication/libcomm_utils/libcomm_memory.cpp'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--git-ref')
    parser.add_argument('--cxx', default='g++')
    args = parser.parse_args()
    source = (subprocess.check_output(['git', 'show', args.git_ref + ':' + SOURCE],
              cwd=ROOT, text=True) if args.git_ref else (ROOT / SOURCE).read_text())
    start = source.index('char gs_find_memory_data(StreamState* node, int* waitnode_count)')
    end = source.index('\n}\n', start) + 3
    body = source[start:end]
    harness = (HERE / 'harness.cpp').read_text()
    assert harness.count('/* PRODUCTION_FUNCTION */') == 1
    with tempfile.TemporaryDirectory(prefix='local-stream-eof-') as temporary:
        executable = str(Path(temporary) / 'check')
        subprocess.run([args.cxx, '-std=c++11', '-O2', '-Wall', '-Wextra', '-Werror',
                        '-x', 'c++', '-', '-o', executable],
                       input=harness.replace('/* PRODUCTION_FUNCTION */', body),
                       text=True, check=True)
        completed = subprocess.run([executable], check=False)
        raise SystemExit(completed.returncode)


if __name__ == '__main__':
    main()
