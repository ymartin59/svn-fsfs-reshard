# svn-fsfs-reshard

This Subversion FSFS shard administration tool allows to know how a repository
FSFS is organized (linear or sharded layout, logical or physical addressing)
and tune shard size for better performance.

It may be use to migrate an upgraded repository from linear to sharded layout,
but still with physical addressing.

## How to tune Subversion repository shard size

These steps can be apply on your repository at any time to improve performance.
If it has been already packed, the script will unpack it before reshard to a
new shard size, you will have to run again svnadmin pack repository after
reshard.

Prerequisites : your repository is offline, you have just generated a backup of
it.

- Check your repository format version and mode with `fsfs-reshard.py
  repository`. No change on repository yet. A 1.5 or 1.6 repository can be in
  linear mode, sharded mode or sharded mode with packed shards.

- Estimate packed shard file sizes with `fsfs-reshard.py repository
  target=1000`.  No change on repository yet. The default Subversion shard size
  is 1000 revisions per shard.  Maybe it is not suitable to your specific
  repository if file sizes are be too large after packing.

- When your target shard size suits you, run `fsfs-reshard.py repository 1000`.
  Job is done. Repository revisions are now collated into 1000-items groups.

- Check your repository format version and mode with `fsfs-reshard.py repository`.
  No change on repository. You should get Current FSFS db format version 4 with
  sharded layout, max files per shard: 1000.  and the list of effective shard
  sizes.

- To improve disk access, you can pack complete shards into large files with
  svnadmin pack repository

- Check your repository format version and packed mode with `fsfs-reshard.py
  repository`. No change on repository.

- Run `svnadmin verify repository` to check integrity. If it fails, you
  probably have to get back your repository backup and use the dump/load
  method.

- Set the repository online again.
