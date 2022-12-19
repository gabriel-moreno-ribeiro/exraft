defmodule Raft do
  @moduledoc """
  Raft consensus written from scratch in Elixir, replicating a key-value store.

      ids = Raft.Cluster.start(["a", "b", "c"])
      {:ok, leader} = Raft.Cluster.wait_for_leader(ids)
      {:ok, _index} = Raft.Server.command(leader, {:put, :answer, 42})
      {:ok, 42} = Raft.Server.read(leader, :answer)
  """

  defdelegate start(ids), to: Raft.Cluster
  defdelegate leader(ids), to: Raft.Cluster
  defdelegate command(ids, cmd), to: Raft.Cluster
end
