defmodule Raft.Server do
  @moduledoc """
  One Raft server (Ongaro & Ousterhout, "In Search of an Understandable
  Consensus Algorithm") replicating a key-value store.

  Roles: follower, candidate, leader. Followers time out into candidates,
  candidates ask for votes, a majority makes a leader, the leader sends
  heartbeats (`append_entries`) and replicates client commands. An entry is
  committed once a majority has stored it, then applied to the store.

  Everything is a GenServer; RPCs are plain messages routed through
  `Raft.Network` so tests can partition the cluster.
  """

  use GenServer
  require Logger

  @election_min 150
  @election_max 300
  @heartbeat 50

  defstruct id: nil,
            peers: [],
            role: :follower,
            term: 0,
            voted_for: nil,
            log: %{},
            last_index: 0,
            commit_index: 0,
            last_applied: 0,
            next_index: %{},
            match_index: %{},
            votes: MapSet.new(),
            leader: nil,
            election_timer: nil,
            heartbeat_timer: nil,
            store: %{},
            pending: %{},
            pending_reads: [],
            term_start: 0

  # ---------------------------------------------------------------------------
  # client API
  # ---------------------------------------------------------------------------

  def start_link(opts) do
    id = Keyword.fetch!(opts, :id)
    GenServer.start_link(__MODULE__, opts, name: via(id))
  end

  def via(id), do: {:via, Registry, {Raft.Registry, id}}

  @doc "Submits a command (`{:put, k, v}` or `{:delete, k}`); replies once committed."
  def command(id, cmd, timeout \\ 5_000) do
    GenServer.call(via(id), {:command, cmd}, timeout)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @doc "Reads a key from the leader's applied state (followers refuse, to keep reads consistent)."
  def read(id, key, timeout \\ 5_000) do
    GenServer.call(via(id), {:read, key}, timeout)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @doc "Inspection: role, term, leader, commit index, log length, store."
  def info(id) do
    GenServer.call(via(id), :info)
  catch
    :exit, _ -> nil
  end

  def stop(id) do
    GenServer.stop(via(id))
  catch
    :exit, _ -> :ok
  end

  # ---------------------------------------------------------------------------
  # lifecycle
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    id = Keyword.fetch!(opts, :id)
    peers = Keyword.fetch!(opts, :peers) -- [id]
    state = %__MODULE__{id: id, peers: peers}
    {:ok, reset_election_timer(state)}
  end

  @impl true
  def handle_call({:command, cmd}, from, %{role: :leader} = state) do
    index = state.last_index + 1
    entry = %{index: index, term: state.term, command: cmd}

    state =
      %{
        state
        | log: Map.put(state.log, index, entry),
          last_index: index,
          pending: Map.put(state.pending, index, from)
      }
      |> Map.update!(:match_index, &Map.put(&1, state.id, index))

    # a single-node cluster commits immediately
    state = if state.peers == [], do: advance_commit(state), else: send_appends(state)
    {:noreply, state}
  end

  def handle_call({:command, _cmd}, _from, state) do
    {:reply, {:error, {:not_leader, state.leader}}, state}
  end

  # A new leader may still hold uncommitted entries from earlier terms; it
  # answers reads only after its own no-op entry commits, which commits them.
  def handle_call({:read, key}, from, %{role: :leader} = state) do
    if state.commit_index >= state.term_start do
      {:reply, {:ok, Map.get(state.store, key)}, state}
    else
      {:noreply, %{state | pending_reads: [{from, key} | state.pending_reads]}}
    end
  end

  def handle_call({:read, _key}, _from, state) do
    {:reply, {:error, {:not_leader, state.leader}}, state}
  end

  def handle_call(:info, _from, state) do
    info = %{
      id: state.id,
      role: state.role,
      term: state.term,
      leader: state.leader,
      commit_index: state.commit_index,
      last_index: state.last_index,
      log: for(i <- 1..state.last_index//1, do: state.log[i]),
      store: state.store
    }

    {:reply, info, state}
  end

  # ---------------------------------------------------------------------------
  # timers
  # ---------------------------------------------------------------------------

  @impl true
  def handle_info(:election_timeout, %{role: :leader} = state), do: {:noreply, state}

  def handle_info(:election_timeout, state) do
    # become a candidate: new term, vote for self, ask everyone else
    term = state.term + 1

    state =
      %{
        state
        | role: :candidate,
          term: term,
          voted_for: state.id,
          votes: MapSet.new([state.id]),
          leader: nil
      }
      |> fail_pending()
      |> reset_election_timer()

    if state.peers == [] do
      {:noreply, become_leader(state)}
    else
      last_term = last_term(state)

      for peer <- state.peers do
        Raft.Network.send(
          state.id,
          peer,
          {:request_vote, term, state.id, state.last_index, last_term}
        )
      end

      {:noreply, state}
    end
  end

  def handle_info(:heartbeat, %{role: :leader} = state) do
    {:noreply, state |> send_appends() |> schedule_heartbeat()}
  end

  def handle_info(:heartbeat, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # RequestVote
  # ---------------------------------------------------------------------------

  def handle_info({:request_vote, term, candidate, cand_last_index, cand_last_term}, state) do
    state = if term > state.term, do: step_down(state, term), else: state

    up_to_date =
      cand_last_term > last_term(state) or
        (cand_last_term == last_term(state) and cand_last_index >= state.last_index)

    grant = term == state.term and state.voted_for in [nil, candidate] and up_to_date

    state = if grant, do: %{state | voted_for: candidate} |> reset_election_timer(), else: state

    Raft.Network.send(state.id, candidate, {:request_vote_reply, state.term, grant, state.id})
    {:noreply, state}
  end

  def handle_info({:request_vote_reply, term, granted, from}, %{role: :candidate} = state) do
    cond do
      term > state.term ->
        {:noreply, step_down(state, term)}

      term == state.term and granted ->
        votes = MapSet.put(state.votes, from)
        state = %{state | votes: votes}

        if MapSet.size(votes) > div(length(state.peers) + 1, 2) do
          {:noreply, become_leader(state)}
        else
          {:noreply, state}
        end

      true ->
        {:noreply, state}
    end
  end

  def handle_info({:request_vote_reply, term, _granted, _from}, state) do
    {:noreply, if(term > state.term, do: step_down(state, term), else: state)}
  end

  # ---------------------------------------------------------------------------
  # AppendEntries
  # ---------------------------------------------------------------------------

  def handle_info(
        {:append_entries, term, leader, prev_index, prev_term, entries, leader_commit},
        state
      ) do
    if term < state.term do
      Raft.Network.send(
        state.id,
        leader,
        {:append_entries_reply, state.term, false, state.last_index, state.id}
      )

      {:noreply, state}
    else
      state =
        state
        |> step_down(term)
        |> Map.put(:leader, leader)
        |> reset_election_timer()

      prev_ok =
        prev_index == 0 or
          (prev_index <= state.last_index and state.log[prev_index].term == prev_term)

      if prev_ok do
        state = append_new_entries(state, prev_index, entries)
        last_new = prev_index + length(entries)

        state =
          if leader_commit > state.commit_index do
            %{state | commit_index: min(leader_commit, max(last_new, state.commit_index))}
            |> apply_committed()
          else
            state
          end

        Raft.Network.send(
          state.id,
          leader,
          {:append_entries_reply, state.term, true, last_new, state.id}
        )

        {:noreply, state}
      else
        # tell the leader how far our log goes so it can back off quickly
        hint = min(state.last_index, max(prev_index - 1, 0))

        Raft.Network.send(
          state.id,
          leader,
          {:append_entries_reply, state.term, false, hint, state.id}
        )

        {:noreply, state}
      end
    end
  end

  def handle_info({:append_entries_reply, term, success, match, from}, %{role: :leader} = state) do
    cond do
      term > state.term ->
        {:noreply, step_down(state, term)}

      term < state.term ->
        {:noreply, state}

      success ->
        state = %{
          state
          | match_index: Map.update(state.match_index, from, match, &max(&1, match)),
            next_index: Map.put(state.next_index, from, match + 1)
        }

        {:noreply, advance_commit(state)}

      true ->
        next = max(1, min(state.next_index[from] - 1, match + 1))
        state = %{state | next_index: Map.put(state.next_index, from, next)}
        {:noreply, send_append(state, from)}
    end
  end

  def handle_info({:append_entries_reply, _term, _success, _match, _from}, state),
    do: {:noreply, state}

  def handle_info(_other, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # helpers
  # ---------------------------------------------------------------------------

  defp become_leader(state) do
    Logger.debug("#{inspect(state.id)} is leader for term #{state.term}")

    # a no-op entry from this term lets the leader commit everything before it
    index = state.last_index + 1
    noop = %{index: index, term: state.term, command: :noop}
    next = Map.new(state.peers, &{&1, index})
    match = state.peers |> Map.new(&{&1, 0}) |> Map.put(state.id, index)

    %{
      state
      | role: :leader,
        leader: state.id,
        next_index: next,
        match_index: match,
        votes: MapSet.new(),
        log: Map.put(state.log, index, noop),
        last_index: index,
        term_start: index
    }
    |> cancel_election_timer()
    |> then(fn s -> if s.peers == [], do: advance_commit(s), else: send_appends(s) end)
    |> schedule_heartbeat()
  end

  # Any message with a higher term (or an AppendEntries from a legitimate
  # leader) turns us into a follower of that term.
  defp step_down(state, term) do
    was_leader = state.role == :leader

    state =
      if term > state.term do
        %{state | term: term, voted_for: nil, role: :follower, leader: nil}
      else
        %{state | role: :follower}
      end

    state = if was_leader, do: fail_pending(state), else: state
    state = if was_leader, do: cancel_heartbeat(state), else: state
    if was_leader, do: reset_election_timer(state), else: state
  end

  defp fail_pending(state) do
    for {_index, from} <- state.pending, do: GenServer.reply(from, {:error, :lost_leadership})
    for {from, _key} <- state.pending_reads, do: GenServer.reply(from, {:error, :lost_leadership})
    %{state | pending: %{}, pending_reads: []}
  end

  defp answer_reads(%{role: :leader} = state) when state.commit_index >= state.term_start do
    for {from, key} <- state.pending_reads,
        do: GenServer.reply(from, {:ok, Map.get(state.store, key)})

    %{state | pending_reads: []}
  end

  defp answer_reads(state), do: state

  defp send_appends(state) do
    Enum.reduce(state.peers, state, fn peer, acc -> send_append(acc, peer) end)
  end

  defp send_append(state, peer) do
    next = Map.get(state.next_index, peer, state.last_index + 1)
    prev_index = next - 1
    prev_term = if prev_index == 0, do: 0, else: state.log[prev_index].term
    entries = for i <- next..state.last_index//1, do: state.log[i]

    Raft.Network.send(
      state.id,
      peer,
      {:append_entries, state.term, state.id, prev_index, prev_term, entries, state.commit_index}
    )

    state
  end

  defp append_new_entries(state, _prev_index, []), do: state

  defp append_new_entries(state, prev_index, entries) do
    Enum.reduce(entries, state, fn entry, acc ->
      case acc.log[entry.index] do
        %{term: t} when t == entry.term ->
          acc

        _ ->
          # conflict (or new): drop everything from here on and append
          log =
            acc.log
            |> Enum.reject(fn {i, _} -> i >= entry.index end)
            |> Map.new()
            |> Map.put(entry.index, entry)

          %{acc | log: log, last_index: entry.index}
      end
    end)
    |> then(fn acc -> %{acc | last_index: max(acc.last_index, prev_index + length(entries))} end)
  end

  # Leader: commit the highest index replicated on a majority, restricted to
  # entries from the current term (Raft section 5.4.2).
  defp advance_commit(state) do
    cluster_size = length(state.peers) + 1

    candidate =
      state.commit_index..state.last_index//1
      |> Enum.reverse()
      |> Enum.find(state.commit_index, fn n ->
        n > state.commit_index and
          state.log[n].term == state.term and
          Enum.count(state.match_index, fn {_id, m} -> m >= n end) * 2 > cluster_size
      end)

    if candidate > state.commit_index do
      %{state | commit_index: candidate} |> apply_committed()
    else
      state
    end
  end

  defp apply_committed(%{last_applied: applied, commit_index: commit} = state)
       when applied >= commit,
       do: answer_reads(state)

  defp apply_committed(state) do
    index = state.last_applied + 1
    entry = state.log[index]

    store =
      case entry.command do
        {:put, k, v} -> Map.put(state.store, k, v)
        {:delete, k} -> Map.delete(state.store, k)
        _ -> state.store
      end

    {from, pending} = Map.pop(state.pending, index)
    if from, do: GenServer.reply(from, {:ok, index})

    apply_committed(%{state | store: store, last_applied: index, pending: pending})
  end

  defp last_term(%{last_index: 0}), do: 0
  defp last_term(state), do: state.log[state.last_index].term

  defp reset_election_timer(state) do
    state = cancel_election_timer(state)
    ref = Process.send_after(self(), :election_timeout, Enum.random(@election_min..@election_max))
    %{state | election_timer: ref}
  end

  defp cancel_election_timer(%{election_timer: nil} = state), do: state

  defp cancel_election_timer(state) do
    Process.cancel_timer(state.election_timer)
    %{state | election_timer: nil}
  end

  defp schedule_heartbeat(state) do
    state = cancel_heartbeat(state)
    %{state | heartbeat_timer: Process.send_after(self(), :heartbeat, @heartbeat)}
  end

  defp cancel_heartbeat(%{heartbeat_timer: nil} = state), do: state

  defp cancel_heartbeat(state) do
    Process.cancel_timer(state.heartbeat_timer)
    %{state | heartbeat_timer: nil}
  end
end
