defmodule Raft.Cluster do
  @moduledoc "Helpers to start a cluster of servers in one VM and find its leader."

  @doc "Starts servers with the given ids, each knowing about all the others."
  def start(ids) do
    for id <- ids do
      {:ok, _pid} = Raft.Server.start_link(id: id, peers: ids)
    end

    ids
  end

  def stop(ids) when is_list(ids), do: Enum.each(ids, &Raft.Server.stop/1)
  def stop(id), do: Raft.Server.stop(id)

  @doc "Ids of the servers that are currently alive."
  def alive(ids), do: Enum.filter(ids, &(Registry.lookup(Raft.Registry, &1) != []))

  @doc "The current leader among the alive servers, or nil."
  def leader(ids) do
    ids
    |> alive()
    |> Enum.map(&Raft.Server.info/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.filter(&(&1.role == :leader))
    |> Enum.max_by(& &1.term, fn -> nil end)
    |> case do
      nil -> nil
      info -> info.id
    end
  end

  @doc "Polls until exactly one alive server calls itself leader and the others agree, or times out."
  def wait_for_leader(ids, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait(ids, deadline)
  end

  defp do_wait(ids, deadline) do
    infos = ids |> alive() |> Enum.map(&Raft.Server.info/1) |> Enum.reject(&is_nil/1)
    leaders = Enum.filter(infos, &(&1.role == :leader))
    max_term = infos |> Enum.map(& &1.term) |> Enum.max(fn -> 0 end)
    current = Enum.filter(leaders, &(&1.term == max_term))

    cond do
      length(current) == 1 and
          Enum.all?(infos, &(&1.leader == hd(current).id or &1.role == :leader)) ->
        {:ok, hd(current).id}

      System.monotonic_time(:millisecond) > deadline ->
        {:error, :timeout}

      true ->
        Process.sleep(20)
        do_wait(ids, deadline)
    end
  end

  @doc "Sends a command to whichever server is leader, following redirects."
  def command(ids, cmd, attempts \\ 50) do
    case wait_for_leader(ids) do
      {:ok, leader} ->
        case Raft.Server.command(leader, cmd) do
          {:ok, index} ->
            {:ok, index}

          _error when attempts > 0 ->
            Process.sleep(50)
            command(ids, cmd, attempts - 1)

          error ->
            error
        end

      error ->
        error
    end
  end

  @doc "Polls until every alive server has applied the same store, returns it."
  def wait_for_convergence(ids, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_converge(ids, deadline)
  end

  defp do_converge(ids, deadline) do
    infos = ids |> alive() |> Enum.map(&Raft.Server.info/1) |> Enum.reject(&is_nil/1)
    stores = infos |> Enum.map(& &1.store) |> Enum.uniq()
    logs = infos |> Enum.map(& &1.log) |> Enum.uniq()

    cond do
      length(stores) == 1 and length(logs) == 1 ->
        {:ok, hd(stores)}

      System.monotonic_time(:millisecond) > deadline ->
        {:error, {:diverged, stores}}

      true ->
        Process.sleep(20)
        do_converge(ids, deadline)
    end
  end
end
