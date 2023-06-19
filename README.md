[![GoDoc](https://godoc.org/github.com/KarpelesLab/clouddb?status.svg)](https://godoc.org/github.com/KarpelesLab/clouddb)

# CloudDB

Another attempt at building a fault tolerant storage system.

This provides no guarantee of ACID compliance, no transaction isolation and such.

It can store JSON objects only.

* Primary keys will need to be globally unique, using for example uuid.NewRandom()
* JSON object contains a number of values, always start with '{' (no support for root to be anything else than an object)
* Search index keys can be specified and will be replicated globally
* Any update will be transfromed into a delta JSON and replicated as delta only (TODO)
* All updates will have a timestamp and be applied based on timestamp value
* A global log of all updates is stored and kept for a limited amount of time (TODO cleanup of old records)
* All checkpoints also have a log count value that represents the size of work so far and allows detecting missing logs
* Local data is stored in a single leveldb in order to allow consistent snapshots

Database consistency:

* Log entries sorted by nanotime timestamp
* Checkpoints will include number of known log entries
* We produce one checkpoint per 24 hours by default in order to ensure that even if a server goes down it can come back up

## Leveldb prefixes

leveldb prefixes are fixed at 3 bytes for ease of storage.

* idx+key = id (indices)
* log+logid = log (journal, indexed by timestamp + hash)
* dat+id = data (data)
* nfo+id = version (16 bytes current record version)
* kdt+id = keys (key data, 32 bits length of key followed by key, repeated for each key)
* typ+type_name = type data
* chk+id = checkpoint

### Journal

Key: `log` + timestamp(16bytes) + hash(sha256)

## checkpoints

* We perform one checkpoint every 24 hours
* Checkpoint is rounded timestamp + number of log entries in the past 24 hours + xor of log keys
* We only keep 100 latest checkpoints (3+ months)

## snapshots

* We perform a snapshot within a few minutes of a checkpoint being reached
* The snapshot will include all the changes up to the checkpoint, and some more
* Upon starting from zero, a node will load the latest snapshot and download it, then apply subsequent log except for log already included in the snapshot since considered already applied
* Log entries prior to the snapshot date will not be included in the snapshot
