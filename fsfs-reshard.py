#!/usr/bin/env python
#  -*- coding: utf-8 -*-
#
# fsfs-reshard.py REPOS_PATH
# fsfs-reshard.py REPOS_PATH -target=MAX_FILES_PER_SHARD
#
# Display repository information about fsfs db.
#
# fsfs-reshard.py REPOS_PATH MAX_FILES_PER_SHARD
#
# Perform an offline conversion of an FSFS repository between linear (format
# 2, usable by Subversion 1.4+) and sharded (format 3,4,6), usable by Subversion
# 1.5+) layouts.
#
# The MAX_FILES_PER_SHARD argument specifies the maximum number of files
# that will be stored in each shard (directory), or zero to specify a linear
# layout.  Subversion 1.5 uses a default value of 1000 files per shard.
#
# As the repository will not be valid while the conversion is in progress,
# the repository administrator must ensure that access to the repository is
# blocked for the duration of the conversion.
#
# In the event that the conversion is interrupted, the repository will be in
# an inconsistent state.  The repository administrator should then re-run
# this tool to completion.
#
#
# Note that, currently, resharding from one sharded layout to another is
# likely to be an extremely slow process.  To reshard, we convert from a
# sharded to linear layout and then to the new sharded layout.  The problem
# is that the initial conversion to the linear layout triggers exactly the
# same 'large number of files in a directory' problem that sharding is
# intended to solve.
#
# ====================================================================
#    Licensed to the Apache Software Foundation (ASF) under one
#    or more contributor license agreements.  See the NOTICE file
#    distributed with this work for additional information
#    regarding copyright ownership.  The ASF licenses this file
#    to you under the Apache License, Version 2.0 (the
#    "License"); you may not use this file except in compliance
#    with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing,
#    software distributed under the License is distributed on an
#    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#    KIND, either express or implied.  See the License for the
#    specific language governing permissions and limitations
#    under the License.
# ====================================================================
#
# Subversion filesystem format 4 and 6 support for versions 1.6, 1.7, 1.8,
# unpack operation and shard file computation contributed by Yves Martin
# (ymartin1040 0x40 gmail 0x2E com)
#
# $HeadURL: http://svn.apache.org/repos/asf/subversion/trunk/tools/server-side/fsfs-reshard.py $
# $LastChangedDate: 2009-11-16 20:07:17 +0100 (Mon, 16 Nov 2009) $
# $LastChangedBy: hwright $
# $LastChangedRevision: 880911 $

import os, stat, sys, shutil

from errno import EEXIST
from sets import Set
import ConfigParser

def usage():
  """Print a usage message and exit."""
  print("""usage: %s REPOS_PATH [target=MAX_FILES_PER_SHARD]

Computes shard sizes for current repository or for a target
MAX_FILES_PER_SHARD to tune this parameter according to
performance criteria.

usage: %s REPOS_PATH MAX_FILES_PER_SHARD [START END]

Perform an offline conversion of an FSFS repository between linear
(readable by Subversion 1.4 or later) and sharded (readable by
Subversion 1.5 or later) layouts.

It is recommended to first upgrade your repository to your current
Subversion release with 'svnadmin upgrade REPOS_PATH'.

Packed shards are unpacked before converting. According to your
needs, you may want to invoke 'svnadmin pack REPOS_PATH' after.

The MAX_FILES_PER_SHARD argument specifies the maximum number of
files that will be stored in each shard (directory), or zero to
specify a linear layout.  Subversion 1.5 uses a default value of
1000 files per shard.

Convert revisions START through END inclusive if specified, or all
revisions if unspecified.
""" % sys.argv[0])
  sys.exit(1)

def incompatible_repos_format(repos_path, format):
  """Print an error saying that REPOS_PATH is a repository with an
  incompatible repository format FORMAT, then exit."""
  sys.stderr.write("""error: unable to convert repository '%s'.

This repository is not compatible with this tool.  Valid
repository formats are '3' or '5'; this repository is
format '%s'.

""" % (repos_path, format))
  sys.stderr.flush()
  sys.exit(1)

def incompatible_fs_format(repos_path, format):
  """Print an error saying that REPOS_PATH is a repository with an
  incompatible filesystem format FORMAT, then exit."""
  sys.stderr.write("""error: unable to open repository '%s'.

This repository contains a filesystem that is not compatible with
this tool.  Valid filesystem formats are '1', '2', '3', '4' or '6'; this
repository contains a filesystem with format '%s'.

Compressed packed revprops is not supported.

""" % (repos_path, format))
  sys.stderr.flush()
  sys.exit(1)

def unexpected_fs_format_options(repos_path):
  """Print an error saying that REPOS_PATH is a repository with
  unexpected filesystem format options, then exit."""
  sys.stderr.write("""error: unable to open repository '%s'.

This repository contains a filesystem that appears to be invalid -
there is unexpected data after the filesystem format number.

""" % repos_path)
  sys.stderr.flush()
  sys.exit(1)

def incompatible_fs_format_option(repos_path, option):
  """Print an error saying that REPOS_PATH is a repository with an
  incompatible filesystem format option OPTION, then exit."""
  sys.stderr.write("""error: unable to convert repository '%s'.

This repository contains a filesystem that is not compatible with
this tool.  This tool recognises the 'layout' option but the
filesystem uses the '%s' option.

""" % (repos_path, option))
  sys.stderr.flush()
  sys.exit(1)

def warn_about_fs_format_1(repos_path, format_path):
  """Print a warning saying that REPOS_PATH contains a format 1 FSFS
  filesystem that we can't reconstruct, then exit."""
  sys.stderr.write("""warning: conversion of '%s' will be one-way.

This repository is currently readable by Subversion 1.1 or later.
This tool can convert this repository to one that is readable by
either Subversion 1.4 (or later) or Subversion 1.5 (or later),
but it is not able to convert it back to the original format - a
separate dump/load step would be required.

If you would like to upgrade this repository anyway, delete the
file '%s' and re-run this tool.

""" % (repos_path, format_path))
  sys.stderr.flush()
  sys.exit(1)

def check_repos_format(repos_path):
  """Check that REPOS_PATH contains a repository with a suitable format;
  print a message and exit if not."""
  format_path = os.path.join(repos_path, 'format')
  try:
    format_file = open(format_path)
    format = format_file.readline()
    if not format.endswith('\n'):
      incompatible_repos_format(repos_path, format + ' <missing newline>')
    format = format.rstrip('\n')
    if format == '3' or format == '5':
      pass
    else:
      incompatible_repos_format(repos_path, format)
  except IOError:
    # In all likelihood, the file doesn't exist.
    incompatible_repos_format(repos_path, '<unreadable>')

def is_packed_revprops_compressed(repos_path):
  """Check if repository at REPOS_PATH has compressed revprops enabled."""
  fsfsconf_path = os.path.join(repos_path, 'db', 'fsfs.conf')
  try:
    config = ConfigParser.ConfigParser()
    config.read(fsfsconf_path)
    return config.getboolean('packed-revprops', 'compress-packed-revprops')
  except IOError:
    # In all likelihood, the file doesn't exist.
    incompatible_repos_format(repos_path, '<unreadable>')
  except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
    return False # Likely section is lacking or option is commented

def check_fs_format(repos_path):
  """Check that REPOS_PATH contains a filesystem with a suitable format,
  or that it contains no format file; print a message and exit if neither
  is true.  Return an array [format number, shard size] whether the filesystem is sharded."""
  result = [0, 0]
  db_path = os.path.join(repos_path, 'db')
  format_path = os.path.join(db_path, 'format')
  if not(os.path.exists(format_path)):
    # Recover from format.bak if interrupted
    format_path = os.path.join(db_path, 'format.bak')
    if not(os.path.exists(format_path)):
      sys.stderr.write("error: db/format and db/format.bak missing.\n")
      sys.stderr.flush()
      sys.exit(1)

  try:
    format_file = open(format_path)
    format = format_file.readline()
    if not format.endswith('\n'):
      incompatible_fs_format(repos_path, format + ' <missing newline>')
    format = format.rstrip('\n')
    if format == '1':
      # This is a format 1 (svndiff0 only) filesystem.  We can upgrade it,
      # but we can't downgrade again (since we can't uncompress any of the
      # svndiff1 deltas that may have been written).  Warn the user and exit.
      warn_about_fs_format_1(repos_path, format_path)
    if format == '2' or format == '3' or format == '4' or format == '6':
      pass
    else:
      incompatible_fs_format(repos_path, format)

    result[0] = format;

    for line in format_file:
      if format == '2':
        unexpected_fs_format_options(repos_path)

      line = line.rstrip('\n')
      if line == 'layout linear':
        pass
      elif line.startswith('layout sharded '):
        result[1] = int(line[15:])
      else:
        incompatible_fs_format_option(repos_path, line)

    format_file.close()
  except IOError:
    # The format file might not exist if we've previously been interrupted,
    # or if the user is following our advice about upgrading a format 1
    # repository.  In both cases, we'll just assume the format was
    # compatible.
    pass

  return result

def current_file(repos_path):
  """Return triple of (revision, next_node_id, next_copy_id) from
  REPOS_PATH/db/current ."""
  return open(os.path.join(repos_path, 'db', 'current')).readline().split()

def backup_fs_format(repos_path):
  """Rename the filesystem format file for repository REPOS_PATH.
  Do not raise an error if the file is already renamed."""
  format_path = os.path.join(repos_path, 'db', 'format')
  try:
    statinfo = os.stat(format_path)
  except OSError:
    # The file probably doesn't exist.
    return

  format_bak_path = os.path.join(repos_path, 'db', 'format.bak')
  # On Windows, we need to ensure the file is writable before we can
  # rename/remove it.
  os.chmod(format_path, statinfo.st_mode | stat.S_IWUSR)
  try:
    os.rename(format_path, format_bak_path)
  except OSError:
    # Unexpected but try to go on
    os.remove(format_bak_path)
    os.rename(format_path, format_bak_path)

def write_fs_format(repos_path, contents):
  """Write a new filesystem format file for repository REPOS_PATH containing
  CONTENTS."""
  format_path = os.path.join(repos_path, 'db', 'format')
  format_bak_path = os.path.join(repos_path, 'db', 'format.bak')
  # Permissions and owner/group are preserved with rename
  try:
    os.rename(format_bak_path, format_path)
  except OSError:
    # Unexpected but try to go on
    os.remove(format_path)
  f = open(format_path, 'wb')
  f.write(contents)
  f.close()

def suffix_unpacked_shard(path):
  """Add '.shard' suffix to unpacked shard number directory."""
  for name in os.listdir(path):
    if name.endswith('.shard') or name.endswith('.pack'):
      continue
    subdir_path = os.path.join(path, name)
    if not os.path.isdir(subdir_path):
      continue
    os.rename(subdir_path, subdir_path + '.shard')

def linearise(path):
  """Move all the files in subdirectories of PATH into PATH, and remove the
  subdirectories.  Handle conflicts between subdirectory names and files
  contained in subdirectories by ensuring subdirectories have a '.shard'
  suffix prior to moving (the files are assumed not to have this suffix.
  Abort if a subdirectory is found to contain another subdirectory."""
  suffix_unpacked_shard(path)

  # Now move all the subdirectory contents into the parent and remove
  # the subdirectories.
  for root_path, dirnames, filenames in os.walk(path):
    if root_path == path:
      continue
    if len(dirnames) > 0:
      sys.stderr.write("error: directory '%s' contains other unexpected directories.\n" \
        % root_path)
      sys.stderr.flush()
      sys.exit(1)
    for name in filenames:
      from_path = os.path.join(root_path, name)
      to_path = os.path.join(path, name)
      os.rename(from_path, to_path)
    os.rmdir(root_path)

def shard(path, max_files_per_shard, start, end):
  """Move the files for revisions START to END inclusive in PATH into
  subdirectories of PATH named such that subdirectory '0' contains at most
  MAX_FILES_PER_SHARD files, those named [0, MAX_FILES_PER_SHARD).  Abort if
  PATH is found to contain any entries with non-numeric names."""

  tmp = path + '.reshard'
  try:
    os.mkdir(tmp)
  except OSError, e:
    if e.errno != EEXIST:
      raise

  # Move all entries into shards named N.shard.
  for rev in range(start, end + 1):
    name = str(rev)
    shard = rev // max_files_per_shard
    shard_name = str(shard) + '.shard'

    from_path = os.path.join(path, name)
    to_path = os.path.join(tmp, shard_name, name)
    try:
      os.rename(from_path, to_path)
    except OSError:
      # The most likely explanation is that the shard directory doesn't
      # exist.  Let's create it and retry the rename.
      os.mkdir(os.path.join(tmp, shard_name))
      os.rename(from_path, to_path)

  # Now rename all the shards to remove the suffix.
  skipped = 0
  for name in os.listdir(tmp):
    if not name.endswith('.shard'):
      sys.stderr.write("warning: ignoring unexpected subdirectory '%s'.\n" \
        % os.path.join(tmp, name))
      sys.stderr.flush()
      skipped += 1
      continue
    from_path = os.path.join(tmp, name)
    to_path = os.path.join(path, os.path.basename(from_path)[:-6])
    os.rename(from_path, to_path)
  skipped == 0 and os.rmdir(tmp)

def extract_part(source, start_position, end_position, target_file):
  """Extract source from start to end position to a target file"""

  copy_buffer_size = 4096
  if not os.path.exists(os.path.dirname(target_file)):
    os.makedirs(os.path.dirname(target_file))
  target = open(target_file, 'wb')
  last_position = start_position
  source.seek(last_position)
  while last_position < end_position:
    bytes_tocopy = copy_buffer_size
    if (end_position - last_position) < copy_buffer_size:
      bytes_tocopy = end_position - last_position
    rev_buffer = source.read(bytes_tocopy)
    target.write(rev_buffer)
    last_position += len(rev_buffer)
    if bytes_tocopy < copy_buffer_size:
      break
  target.close()
  return last_position

def unpack_shard(packed_path, shard_number, unpack, first_rev, revs_size):
  """Compute revision sizes in a packed shard at packed_path and unpack revisions
  if unpack is True.  The first revision of the shard has first_rev number.
  Revision sizes are stored in rev_sizes dictionnary.

  """
  manifest = open(os.path.join(packed_path, 'manifest'), 'r')
  pack_path = os.path.join(packed_path, 'pack')
  end_pack = os.path.getsize(pack_path)
  if unpack:
    pack = open(pack_path, 'rb')
  last_position = int(manifest.readline())
  rev_index = first_rev
  while last_position < end_pack:
    # Read next revision start byte in pack file
    try:
      byte_position = int(manifest.readline())
    except ValueError:
      # last revision: end of pack file
      byte_position = end_pack
    revs_size[rev_index] = byte_position - last_position
    if unpack:
      # Extract revision from pack file to corresponding shard
      arev = os.path.join(packed_path, os.path.pardir, str(shard_number), str(rev_index))
      last_position = extract_part(pack, last_position, byte_position, arev)
    else:
      last_position = byte_position
    rev_index += 1
  # Close file descriptors
  manifest.close()
  if unpack:
    pack.close()
  return revs_size

def unpack_revprops_shard(packed_path, shard_number, first_rev):
  """Unpack a single revision properties shard to linear mode"""

  revprops_packfile = {}
  packfiles = Set()
  with open(os.path.join(packed_path, 'manifest'), 'rb') as manifest:
    for revfile in manifest:
      revfile = revfile.rstrip('\n')
      revprops_packfile[first_rev] = revfile
      packfiles.add(revfile)
      first_rev += 1

  for apackfile in packfiles:
    packrevprops = open(os.path.join(packed_path, apackfile), 'rb')
    packrevprops.read(2) # Skip uncompressed content size
    # Parse revprops sizes
    rev = int(packrevprops.readline())
    rev_count = int(packrevprops.readline())
    end_rev = rev + rev_count - 1
    revprop_sizes = {}
    for r in range(rev, rev + rev_count):
      revprop_sizes[r] = int(packrevprops.readline())
    packrevprops.readline()
    last_position = packrevprops.tell()
    while rev <= end_rev:
      end_position = last_position + revprop_sizes[rev]
      if apackfile == revprops_packfile[rev]:
        # Extract revprop declared in manifest
        arev = os.path.join(packed_path, os.path.pardir, str(shard_number), str(rev))
        last_position = extract_part(packrevprops, last_position, end_position, arev)
      else:
        last_position = end_position
      rev += 1
    # Close file descriptors
    packrevprops.close()

def unpack_revprops_shards(revprops_path, current_shard):
  """Unpack revision properties shards"""

  for root_path, dirnames, filenames in os.walk(revprops_path):
    if len(dirnames) > 0:
      for name in dirnames:
        if (not(name.endswith('.pack'))):
          continue
        shard_number = int(name[:-5])
        shard_path = os.path.join(root_path, name)
        unpack_revprops_shard(shard_path, shard_number,
                              current_shard * shard_number if shard_number > 0 else 1)
        # remove x.pack structure
        shutil.rmtree(shard_path)

def compute_rev_sizes(revs_path, current_shard, unpack):
  """Compute revision sizes based on current shard capacity Support either
  linear, sharded or packed revisions.  If unpack is True, packed sharded are
  unpacked too.

  """
  revs_size = {}
  for root_path, dirnames, filenames in os.walk(revs_path):
    if len(filenames) > 0:
      for name in filenames:
        try:
          revnum = int(name)
          revs_size[revnum] = os.path.getsize(os.path.join(root_path, name))
        except ValueError:
          pass
    if len(dirnames) > 0:
      for name in dirnames:
        if (not(name.endswith('.pack'))):
          continue
        shard_number = int(name[:-5])
        shard_path = os.path.join(root_path, name)
        # get revision sizes from packed shard [and unpack]
        revs_size = unpack_shard(shard_path, shard_number, unpack,
                                 current_shard * shard_number, revs_size)
        if unpack:
          # remove x.pack structure
          shutil.rmtree(shard_path)
  return revs_size

def compute_shard_sizes(revs_size, max_files_per_shard):
  """Compute shard sizes based on target max_files_per_shard
  and map of revision size."""
  current_shard = 0
  current_shard_size = 0
  min_shard_size = 2**63
  max_shard_size = 0
  shard_size_sum = 0
  for i, size in revs_size.iteritems():
    current_shard_size += size
    if ((i + 1) % max_files_per_shard) == 0:
      print 'Shard %d size: %d' % (current_shard, current_shard_size)
      shard_size_sum += current_shard_size
      if current_shard_size < min_shard_size:
        min_shard_size = current_shard_size
      if current_shard_size > max_shard_size:
        max_shard_size = current_shard_size
      current_shard_size = 0
      current_shard += 1
  if current_shard_size != 0:
    print 'Shard %d size: %d' % (current_shard, current_shard_size)
  if current_shard > 0:
    print 'Average full-shard size %d. Minimum: %d, Maximum: %d.' \
          % ((shard_size_sum / current_shard), min_shard_size, max_shard_size)

def reset_min_unpacked(min_unpacked_rev_path):
  """Reset min-unpacked-rev after unpack."""
  with open(min_unpacked_rev_path, 'wb') as min_unpacked_rev_file:
    min_unpacked_rev_file.write('0\n')

def print_estimate_shards(repos_path, fs_format, min_unpacked_rev):
  """Print repository information and computes shard sizes for the specified
  target

  """
  fs_format = check_fs_format(repos_path)
  target_shard = fs_format[1]
  if len(sys.argv) == 3 and sys.argv[2].startswith('target='):
    try:
      target_shard = int(sys.argv[2][7:]) # Remove "target="
    except ValueError, OverflowError:
      sys.stderr.write("error: target maximum files per shard ('%s') is not a valid number.\n" \
                       % sys.argv[2])
      sys.stderr.flush()
      sys.exit(1)
  revs_path = os.path.join(repos_path, 'db', 'revs')
  sys.stdout.write("Current FSFS db format version ")
  sys.stdout.write(fs_format[0])
  if fs_format[1] > 0:
    sys.stdout.write(" with sharded layout, max files per shard: ")
    sys.stdout.write(str(fs_format[1]))
    if min_unpacked_rev > 0:
      sys.stdout.write(", packed shards: ")
      sys.stdout.write(str(min_unpacked_rev / fs_format[1]))
  else:
    sys.stdout.write(" with linear layout")
  if target_shard > 0:
    sys.stdout.write(".\nList of shard sizes for max files per shard = ")
    sys.stdout.write(str(target_shard))
    sys.stdout.write("\n")
    revs_size = compute_rev_sizes(revs_path, fs_format[1], False)
    compute_shard_sizes(revs_size, target_shard)
  else:
    sys.stdout.write(".\n")
  sys.stdout.flush()

def main():
  if len(sys.argv) < 2:
    usage()

  repos_path = sys.argv[1]

  # Get [number format, sharded]
  fs_format = check_fs_format(repos_path)

  # Get minimum unpacked revision, Subversion >= 1.6
  min_unpacked_rev = 0
  min_unpacked_rev_path = os.path.join(repos_path, 'db', 'min-unpacked-rev')
  if os.path.exists(min_unpacked_rev_path):
    min_unpacked_rev_file = open(min_unpacked_rev_path)
    try:
      min_unpacked_rev = int(min_unpacked_rev_file.readline())
    except ValueError, OverflowError:
      sys.stderr.write("error: repository db/min-unpacked-rev does not contain a valid number.\n")
      sys.stderr.flush()
      sys.exit(1)
    min_unpacked_rev_file.close()

  try:
    start = int(sys.argv[3])
    end = int(sys.argv[4])
  except IndexError:
    start = 0
    end = int(current_file(repos_path)[0])

  # Validate the command-line arguments.
  db_path = os.path.join(repos_path, 'db')
  current_path = os.path.join(db_path, 'current')
  if not os.path.exists(current_path):
    sys.stderr.write("error: '%s' doesn't appear to be a Subversion FSFS repository.\n" \
      % repos_path)
    sys.stderr.flush()
    sys.exit(1)

  if len(sys.argv) == 2 or (len(sys.argv) == 3 and sys.argv[2].startswith('target=')):
    print_estimate_shards(repos_path, fs_format, min_unpacked_rev)
    sys.exit(0)

  try:
    max_files_per_shard = int(sys.argv[2])
  except ValueError, OverflowError:
    sys.stderr.write("error: maximum files per shard ('%s') is not a valid number.\n" \
      % sys.argv[2])
    sys.stderr.flush()
    sys.exit(1)

  if max_files_per_shard < 0:
    sys.stderr.write("error: maximum files per shard ('%d') must not be negative.\n" \
      % max_files_per_shard)
    sys.stderr.flush()
    sys.exit(1)

  # Check the format of the repository.
  check_repos_format(repos_path)

  reshard = max_files_per_shard != fs_format[1]
  if not reshard:
    print_estimate_shards(repos_path, fs_format, min_unpacked_rev)
    sys.exit(0)

  # Let the user know what's going on.
  if max_files_per_shard > 0:
    print("Converting '%s' to a sharded structure with %d files per directory" \
      % (repos_path, max_files_per_shard))
    if min_unpacked_rev > 0:
      print('(will unpack)')
    if reshard:
      print('(will convert to a linear structure)')
  else:
    print("Converting '%s' to a linear structure" % repos_path)

  # Prevent access to the repository for the duration of the conversion.
  # There's no clean way to do this, but since the format of the repository
  # is indeterminate, let's remove the format file while we're converting.
  print('- marking the repository as invalid')
  backup_fs_format(repos_path)

  # First, convert to a linear scheme (this makes recovery easier)
  if fs_format[1] > 0:
    revs_path = os.path.join(repos_path, 'db', 'revs')
    if min_unpacked_rev > 0:
      # First unpack
      if is_packed_revprops_compressed(repos_path):
        incompatible_fs_format(repos_path, fs_format[0])
      print('- unpacking db/revs')
      compute_rev_sizes(revs_path, fs_format[1], True)
      print('- unpacking db/revprops')
      unpack_revprops_shards(os.path.join(repos_path, 'db', 'revprops'), fs_format[1])
      min_unpacked_rev = 0
      reset_min_unpacked(min_unpacked_rev_path)
    # If sharding is different, convert to a linear scheme
    if reshard:
      print('- linearising db/revs')
      linearise(revs_path)
      print('- linearising db/revprops')
      linearise(os.path.join(repos_path, 'db', 'revprops'))

  if reshard and max_files_per_shard > 0:
    print('- sharding db/revs')
    shard(os.path.join(repos_path, 'db', 'revs'), max_files_per_shard,
          start, end)
    print('- sharding db/revprops')
    shard(os.path.join(repos_path, 'db', 'revprops'), max_files_per_shard,
          start, end)

  if max_files_per_shard == 0:
    # We're done.  Stamp the filesystem with a format 2/3/4/6 db/format file.
    print('- marking the repository as a valid linear repository')
    format_layout = '\n'
    if fs_format[0] > 2:
      format_layout = '\nlayout linear\n'
    write_fs_format(repos_path, fs_format[0] + format_layout)
  else:
    # Sharded. Keep original 3/4 format or upgrade format 2 to 3.
    target_format = fs_format[0]
    if fs_format[0] == 2:
      target_format = 3
    # We're done.  Stamp the filesystem with a format db/format file.
    print('- marking the repository as a valid sharded repository')
    write_fs_format(repos_path, target_format + '\nlayout sharded %d\n' % max_files_per_shard)

  print('- done.')
  sys.exit(0)

main()

if __name__ == '__main__':
  raise Exception("""This script is unfinished and not ready to be used on live data.
    Trust us. Prepare a backup and run svnadmin verify before putting your repo online""")
