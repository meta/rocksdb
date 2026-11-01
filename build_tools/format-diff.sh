#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
# If clang_format_diff.py command is not specfied, we assume we are able to
# access directly without any path.

print_usage () {
  echo "Usage:"
  echo "format-diff.sh [OPTIONS]"
  echo "-c: check only."
  echo "-y: auto-apply formatting without prompts (non-interactive mode)."
  echo "-h: print this message."
}

while getopts ':cyh' OPTION; do
  case "$OPTION" in
    c)
      CHECK_ONLY=1
      ;;
    y)
      AUTO_APPLY=1
      ;;
    h)
      print_usage
      exit 1
      ;;
    ?)
      print_usage
      exit 1
      ;;
  esac
done

REPO_ROOT="$(git rev-parse --show-toplevel)"

if [ "$CLANG_FORMAT_DIFF" ]; then
  echo "Note: CLANG_FORMAT_DIFF='$CLANG_FORMAT_DIFF'"
  # Dry run to confirm dependencies like argparse
  if $CLANG_FORMAT_DIFF --help >/dev/null < /dev/null; then
    true #Good
  else
    exit 128
  fi
else
  # First try directly executing the possibilities
  if clang-format-diff --help &> /dev/null < /dev/null; then
    CLANG_FORMAT_DIFF=clang-format-diff
  elif clang-format-diff.py --help &> /dev/null < /dev/null; then
    CLANG_FORMAT_DIFF=clang-format-diff.py
  elif $REPO_ROOT/clang-format-diff.py --help &> /dev/null < /dev/null; then
    CLANG_FORMAT_DIFF=$REPO_ROOT/clang-format-diff.py
  else
    # This probably means we need to directly invoke the interpreter.
    # But first find clang-format-diff.py
    if [ -f "$REPO_ROOT/clang-format-diff.py" ]; then
      CFD_PATH="$REPO_ROOT/clang-format-diff.py"
    elif which clang-format-diff.py &> /dev/null; then
      CFD_PATH="$(which clang-format-diff.py)"
    else
      echo "You didn't have clang-format-diff.py and/or clang-format available in your computer!"
      echo "You can download clang-format-diff.py by running: "
      echo "    curl --location https://raw.githubusercontent.com/llvm/llvm-project/main/clang/tools/clang-format/clang-format-diff.py -o ${REPO_ROOT}/clang-format-diff.py"
      echo "You should make sure the downloaded script is not compromised."
      echo "You can download clang-format by running:"
      echo "    brew install clang-format"
      echo "  Or"
      echo "    apt install clang-format"
      echo "  This might work too:"
      echo "    yum install git-clang-format"
      echo "Then make sure clang-format is available and executable from \$PATH:"
      echo "    clang-format --version"
      exit 128
    fi
    # Check argparse pre-req on interpreter, or it will fail
    if echo import argparse | ${PYTHON:-python3}; then
      true # Good
    else
      echo "To run clang-format-diff.py, we'll need the library "argparse" to be"
      echo "installed. You can try either of the follow ways to install it:"
      echo "  1. Manually download argparse: https://pypi.python.org/pypi/argparse"
      echo "  2. easy_install argparse (if you have easy_install)"
      echo "  3. pip install argparse (if you have pip)"
      exit 129
    fi
    # Unfortunately, some machines have a Python2 clang-format-diff.py
    # installed but only a Python3 interpreter installed. Unfortunately,
    # automatic 2to3 migration is insufficient, so suggest downloading latest.
    if grep -q "print '" "$CFD_PATH" && \
       ${PYTHON:-python3} --version | grep -q 'ython 3'; then
      echo "You have clang-format-diff.py for Python 2 but are using a Python 3"
      echo "interpreter (${PYTHON:-python3})."
      echo "You can download clang-format-diff.py for Python 3 by running: "
      echo "    curl --location https://raw.githubusercontent.com/llvm/llvm-project/main/clang/tools/clang-format/clang-format-diff.py -o ${REPO_ROOT}/clang-format-diff.py"
      echo "You should make sure the downloaded script is not compromised."
      exit 130
    fi
    CLANG_FORMAT_DIFF="${PYTHON:-python3} $CFD_PATH"
    # This had better work after all those checks
    if $CLANG_FORMAT_DIFF --help >/dev/null < /dev/null; then
      true #Good
    else
      exit 128
    fi
  fi
fi

# TODO(kailiu) following work is not complete since we still need to figure
# out how to add the modified files done pre-commit hook to git's commit index.
#
# Check if this script has already been added to pre-commit hook.
# Will suggest user to add this script to pre-commit hook if their pre-commit
# is empty.
# PRE_COMMIT_SCRIPT_PATH="`git rev-parse --show-toplevel`/.git/hooks/pre-commit"
# if ! ls $PRE_COMMIT_SCRIPT_PATH &> /dev/null
# then
#   echo "Would you like to add this script to pre-commit hook, which will do "
#   echo -n "the format check for all the affected lines before you check in (y/n):"
#   read add_to_hook
#   if [ "$add_to_hook" == "y" ]
#   then
#     ln -s `git rev-parse --show-toplevel`/build_tools/format-diff.sh $PRE_COMMIT_SCRIPT_PATH
#   fi
# fi
set -e

# Exclude third-party from formatting
EXCLUDE=':!third-party/'

uncommitted_code=`git diff HEAD`

# If there's no uncommitted changes, we assume user are doing post-commit
# format check, in which case we'll try to check the modified lines vs. the
# meta/rocksdb.git main branch. Otherwise, we'll check format of the
# uncommitted code only.
if [ -z "$uncommitted_code" ]
then
  # Attempt to get name of meta/rocksdb.git remote. Older clones might still
  # use the pre-transfer facebook/rocksdb.git URL, which GitHub redirects.
  [ "$FORMAT_REMOTE" ] || FORMAT_REMOTE="$(LC_ALL=POSIX LANG=POSIX git remote -v | grep -E '[:/](meta|facebook)/rocksdb\.git' | head -n 1 | cut -f 1)"
  # Fall back on 'origin' if that fails
  [ "$FORMAT_REMOTE" ] || FORMAT_REMOTE=origin
  # Use main branch from that remote
  [ "$FORMAT_UPSTREAM" ] || FORMAT_UPSTREAM="$FORMAT_REMOTE/$(LC_ALL=POSIX LANG=POSIX git remote show $FORMAT_REMOTE | sed -n '/HEAD branch/s/.*: //p')"
  # Get the common ancestor with that remote branch. Everything after that
  # common ancestor would be considered the contents of a pull request, so
  # should be relevant for formatting fixes.
  FORMAT_UPSTREAM_MERGE_BASE="$(git merge-base "$FORMAT_UPSTREAM" HEAD)"
  # Get the differences
  diffs=$(git diff -U0 "$FORMAT_UPSTREAM_MERGE_BASE" -- $EXCLUDE | $CLANG_FORMAT_DIFF -p 1) || true
  echo "Checking format of changes not yet in $FORMAT_UPSTREAM..."
else
  # Check the format of uncommitted lines,
  diffs=$(git diff -U0 HEAD -- $EXCLUDE | $CLANG_FORMAT_DIFF -p 1) || true
  echo "Checking format of uncommitted changes..."
fi

# Check for missing copyright in new files
echo "Checking for copyright headers in new files..."

# Get list of new files (added, not just modified)
if [ -z "$uncommitted_code" ]; then
  # Post-commit: check files added since merge base
  new_files=$(git diff --name-only --diff-filter=A "$FORMAT_UPSTREAM_MERGE_BASE" -- '*.h' '*.cc' '*.py' $EXCLUDE)
else
  # Pre-commit: check staged new files
  new_files=$(git diff --name-only --diff-filter=A --cached HEAD -- '*.h' '*.cc' '*.py' $EXCLUDE)
fi

if [ -n "$new_files" ]; then
  files_missing_copyright=""

  for file in $new_files; do
    if [ -f "$file" ]; then
      # Check if file is missing copyright
      # For .py files, check for Python-style comment
      # For .h and .cc files, check for C++-style comment
      if [[ "$file" == *.py ]]; then
        if ! grep -q "Copyright (c) Meta Platforms, Inc. and affiliates" "$file"; then
          files_missing_copyright="$files_missing_copyright $file"
          # Add copyright header to Python file
          temp_file=$(mktemp)
          {
            echo "#  Copyright (c) Meta Platforms, Inc. and affiliates."
            echo "#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)"
            echo "#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory)."
            echo
            cat "$file"
          } > "$temp_file"
          mv "$temp_file" "$file"
          echo "Added copyright header to $file"
        fi
      elif [[ "$file" == *.h ]] || [[ "$file" == *.cc ]]; then
        if ! grep -q "Copyright (c) Meta Platforms, Inc. and affiliates" "$file"; then
          files_missing_copyright="$files_missing_copyright $file"
          # Add copyright header to C++ file
          temp_file=$(mktemp)
          {
            echo "//  Copyright (c) Meta Platforms, Inc. and affiliates. "
            echo "//  This source code is licensed under both the GPLv2 (found in the "
            echo "//  COPYING file in the root directory) and Apache 2.0 License "
            echo "//  (found in the LICENSE.Apache file in the root directory)."
            echo
            cat "$file"
          } > "$temp_file"
          mv "$temp_file" "$file"
          echo "Added copyright header to $file"
        fi
      fi
    fi
  done

  if [ -n "$files_missing_copyright" ]; then
    echo "Copyright headers were added to new files."
  else
    echo "All new files have copyright headers."
  fi
else
  echo "No new files to check for copyright headers."
fi

if [ -z "$diffs" ]
then
  echo "Nothing needs to be reformatted!"
  exit 0
elif [ $? -ne 1 ]; then
  # CLANG_FORMAT_DIFF will exit on 1 while there is suggested changes.
  exit $?
elif [ $CHECK_ONLY ]
then
  echo "Your change has unformatted code. Please run make format!"
  if [ $VERBOSE_CHECK ]; then
    clang-format --version
    echo "$diffs"
  fi
  exit 1
fi

# Highlight the insertion/deletion from the clang-format-diff.py's output
COLOR_END="\033[0m"
COLOR_RED="\033[0;31m"
COLOR_GREEN="\033[0;32m"

echo -e "Detect lines that doesn't follow the format rules:\r"
# Add the color to the diff. lines added will be green; lines removed will be red.
echo "$diffs" |
  sed -e "s/\(^-.*$\)/`echo -e \"$COLOR_RED\1$COLOR_END\"`/" |
  sed -e "s/\(^+.*$\)/`echo -e \"$COLOR_GREEN\1$COLOR_END\"`/"

# Handle auto-apply mode (non-interactive)
if [ "$AUTO_APPLY" ]; then
  to_fix="y"
else
  echo -e "Would you like to fix the format automatically (y/n): \c"

  # Make sure under any mode, we can read user input.
  exec < /dev/tty
  read to_fix
fi

if [ "$to_fix" != "y" ]
then
  exit 1
fi

# Do in-place format adjustment.
if [ -z "$uncommitted_code" ]
then
  git diff -U0 "$FORMAT_UPSTREAM_MERGE_BASE" -- $EXCLUDE | $CLANG_FORMAT_DIFF -i -p 1
else
  git diff -U0 HEAD -- $EXCLUDE | $CLANG_FORMAT_DIFF -i -p 1
fi
echo "Files reformatted!"

# Amend to last commit if user do the post-commit format check
# Skip amend prompt in auto-apply mode (user can amend manually if desired)
if [ -z "$uncommitted_code" ] && [ -z "$AUTO_APPLY" ]; then
  echo -e "Would you like to amend the changes to last commit (`git log HEAD --oneline | head -1`)? (y/n): \c"
  read to_amend

  if [ "$to_amend" == "y" ]
  then
    git commit -a --amend --reuse-message HEAD
    echo "Amended to last commit"
  fi
fi
