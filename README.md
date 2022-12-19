# exraft

The Raft consensus algorithm written from scratch in Elixir, replicating a
key-value store across a cluster of GenServers. Leader election, log
replication, commit rules, follower catch-up, leader step-down, and a
network layer with fault injection so the tests can crash nodes and
partition the cluster.

```elixir
ids = Raft.Cluster.start(["a", "b", "c"])
{:ok, leader} = Raft.Cluster.wait_for_leader(ids)

{:ok, index} = Raft.Server.command(leader, {:put, :answer, 42})   # replies once committed
{:ok, 42} = Raft.Server.read(leader, :answer)
{:error, {:not_leader, "a"}} = Raft.Server.command("b", {:put, :x, 1})

Raft.Network.isolate(leader)                # partition the leader away
{:ok, new_leader} = Raft.Cluster.wait_for_leader(ids -- [leader])
Raft.Network.heal()                         # the old leader steps down and catches up
```

```sh
mix test
```

## How it works

- **Roles and terms** (`Raft.Server`): every server starts as a follower with
  a randomised election timeout (150 to 300 ms). If no heartbeat arrives it
  becomes a candidate, increments its term, votes for itself and sends
  `request_vote` to its peers. A vote is granted at most once per term and
  only to candidates whose log is at least as up to date. A majority of
  votes makes a leader, which then sends `append_entries` heartbeats every
  50 ms. Any message carrying a higher term makes a server step down.
- **Log replication**: a client command is appended to the leader's log and
  shipped to followers with the index and term of the preceding entry.
  Followers reject the append when that entry does not match, and the leader
  backs off `next_index` for that follower and retries, so diverging logs
  are repaired from the last point of agreement. Conflicting follower entries
  are truncated.
- **Commit**: the leader commits an index once a majority has stored it,
  restricted to entries from its own term (the safety rule from section
  5.4.2 of the paper). Committed entries are applied in order to the
  key-value store and the waiting client gets `{:ok, index}`. Followers
  learn the commit index from heartbeats.
- **Reads** go through the leader only, so a client never sees a stale
  value from a follower.
- **Network** (`Raft.Network`): servers are registered in a `Registry` and
  send RPCs through one function that can drop messages for isolated nodes
  or cut links. Tests use it to simulate leader crashes, minority
  partitions and healing.

## Tests

The ExUnit suite starts real three-node clusters and checks: exactly one
leader is elected; commands replicate and every store converges; followers
redirect clients; the cluster survives the leader crashing and keeps the
committed data; an isolated leader cannot commit (no split brain) and
steps down when reconnected; logs are identical after partitions heal;
terms are monotonic; a single node commits on its own.

## License

MIT
