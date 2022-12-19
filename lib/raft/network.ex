defmodule Raft.Network do
  @moduledoc """
  Message delivery between servers, with fault injection for tests.

  Servers never send to each other directly; they go through `send/3`, which
  drops the message when either end is isolated or when the pair is cut.
  That is enough to simulate crashes, partitions and healing.
  """

  use Agent

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> %{isolated: MapSet.new(), cut: MapSet.new()} end, name: __MODULE__)
  end

  @doc "Delivers `message` to the server registered as `to`, unless the network says otherwise."
  def send(from, to, message) do
    state = Agent.get(__MODULE__, & &1)

    blocked =
      MapSet.member?(state.isolated, from) or MapSet.member?(state.isolated, to) or
        MapSet.member?(state.cut, pair(from, to))

    unless blocked do
      case Registry.lookup(Raft.Registry, to) do
        [{pid, _}] -> Kernel.send(pid, message)
        [] -> :ok
      end
    end

    :ok
  end

  @doc "Cuts a node off from everyone (it keeps running, nothing gets in or out)."
  def isolate(id), do: Agent.update(__MODULE__, &%{&1 | isolated: MapSet.put(&1.isolated, id)})

  @doc "Cuts the link between two nodes only."
  def cut(a, b), do: Agent.update(__MODULE__, &%{&1 | cut: MapSet.put(&1.cut, pair(a, b))})

  @doc "Removes every partition."
  def heal, do: Agent.update(__MODULE__, fn _ -> %{isolated: MapSet.new(), cut: MapSet.new()} end)

  def reconnect(id),
    do: Agent.update(__MODULE__, &%{&1 | isolated: MapSet.delete(&1.isolated, id)})

  defp pair(a, b), do: if(a <= b, do: {a, b}, else: {b, a})
end
