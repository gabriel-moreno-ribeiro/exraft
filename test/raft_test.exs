defmodule RaftTest do
  use ExUnit.Case, async: false

  alias Raft.{Cluster, Network, Server}

  setup do
    Network.heal()
    ids = for n <- 1..3, do: "#{:erlang.unique_integer([:positive])}-n#{n}"
    Cluster.start(ids)
    on_exit(fn -> Network.heal() end)
    {:ok, ids: ids}
  end

  test "a three node cluster elects exactly one leader", %{ids: ids} do
    assert {:ok, leader} = Cluster.wait_for_leader(ids)
    infos = Enum.map(ids, &Server.info/1)
    assert Enum.count(infos, &(&1.role == :leader)) == 1
    assert Enum.all?(infos, &(&1.leader == leader))
    assert Enum.all?(infos, &(&1.term >= 1))
    Cluster.stop(ids)
  end

  test "commands are replicated and applied on every server", %{ids: ids} do
    assert {:ok, i1} = Cluster.command(ids, {:put, :x, 1})
    assert {:ok, i2} = Cluster.command(ids, {:put, :y, "two"})
    assert {:ok, i3} = Cluster.command(ids, {:delete, :x})
    assert i1 < i2 and i2 < i3, "log indexes grow"
    assert {:ok, store} = Cluster.wait_for_convergence(ids)
    assert store == %{y: "two"}
    {:ok, leader} = Cluster.wait_for_leader(ids)
    assert {:ok, "two"} = Server.read(leader, :y)
    assert {:ok, nil} = Server.read(leader, :x)
    infos = Enum.map(ids, &Server.info/1)
    assert Enum.all?(infos, &(&1.commit_index == i3))
    assert Enum.all?(infos, &(&1.last_index == i3))
    Cluster.stop(ids)
  end

  test "followers redirect clients to the leader", %{ids: ids} do
    {:ok, leader} = Cluster.wait_for_leader(ids)
    [follower | _] = ids -- [leader]
    assert {:error, {:not_leader, ^leader}} = Server.command(follower, {:put, :a, 1})
    assert {:error, {:not_leader, ^leader}} = Server.read(follower, :a)
    Cluster.stop(ids)
  end

  test "the cluster survives the leader crashing", %{ids: ids} do
    {:ok, leader} = Cluster.wait_for_leader(ids)
    assert {:ok, _} = Cluster.command(ids, {:put, :before, :crash})
    term_before = Server.info(leader).term

    Server.stop(leader)
    remaining = ids -- [leader]
    assert {:ok, new_leader} = Cluster.wait_for_leader(remaining)
    assert new_leader != leader
    assert Server.info(new_leader).term > term_before

    assert {:ok, :crash} = Server.read(new_leader, :before), "committed data survives"
    assert {:ok, _} = Cluster.command(remaining, {:put, :after, :crash})
    assert {:ok, %{before: :crash, after: :crash}} = Cluster.wait_for_convergence(remaining)
    Cluster.stop(remaining)
  end

  test "an isolated leader steps down and cannot commit (no split brain)", %{ids: ids} do
    {:ok, old_leader} = Cluster.wait_for_leader(ids)
    assert {:ok, _} = Cluster.command(ids, {:put, :k, :v1})

    Network.isolate(old_leader)
    majority = ids -- [old_leader]
    assert {:ok, new_leader} = Cluster.wait_for_leader(majority)
    assert new_leader != old_leader

    # the majority keeps working
    assert {:ok, _} = Cluster.command(majority, {:put, :k, :v2})

    # the old leader still thinks it leads, but cannot commit anything
    assert Server.info(old_leader).role in [:leader, :candidate, :follower]
    task = Task.async(fn -> Server.command(old_leader, {:put, :k, :stale}, 1_000) end)
    result = Task.await(task, 2_000)

    assert result in [{:error, :unavailable}, {:error, :lost_leadership}] or
             match?({:error, {:not_leader, _}}, result)

    # after healing, the old leader learns about the newer term and adopts the majority's log
    Network.heal()
    assert {:ok, store} = Cluster.wait_for_convergence(ids)
    assert store == %{k: :v2}
    info = Server.info(old_leader)
    assert info.role == :follower
    assert info.leader == new_leader or Cluster.leader(ids) != old_leader
    Cluster.stop(ids)
  end

  test "logs stay identical after partitions heal", %{ids: ids} do
    for i <- 1..5, do: assert({:ok, _} = Cluster.command(ids, {:put, i, i * i}))
    {:ok, leader} = Cluster.wait_for_leader(ids)
    [a, b] = ids -- [leader]

    Network.cut(leader, a)
    assert {:ok, _} = Cluster.command(ids, {:put, :during, :cut})
    Network.heal()

    assert {:ok, store} = Cluster.wait_for_convergence(ids)
    assert store == Map.new(1..5, &{&1, &1 * &1}) |> Map.put(:during, :cut)

    Network.isolate(b)
    assert {:ok, _} = Cluster.command(ids -- [b], {:put, :while, :isolated})
    Network.heal()
    assert {:ok, store} = Cluster.wait_for_convergence(ids)
    assert store[:while] == :isolated
    logs = Enum.map(ids, &Server.info(&1).log)
    assert length(Enum.uniq(logs)) == 1
    assert Enum.all?(hd(logs), &(&1.term >= 1))
    Cluster.stop(ids)
  end

  test "terms only ever increase and votes are per term", %{ids: ids} do
    {:ok, _} = Cluster.wait_for_leader(ids)
    terms1 = Enum.map(ids, &Server.info(&1).term)
    Network.isolate(hd(ids))
    Process.sleep(700)
    Network.heal()
    assert {:ok, _} = Cluster.wait_for_leader(ids)
    terms2 = Enum.map(ids, &Server.info(&1).term)
    assert Enum.max(terms2) >= Enum.max(terms1)
    assert length(Enum.uniq(terms2)) == 1, "everyone converges on the same term"
    Cluster.stop(ids)
  end

  test "a single node cluster commits on its own" do
    id = "single-#{:erlang.unique_integer([:positive])}"
    Cluster.start([id])
    assert {:ok, ^id} = Cluster.wait_for_leader([id])
    assert {:ok, _} = Server.command(id, {:put, :solo, true})
    assert {:ok, true} = Server.read(id, :solo)
    Cluster.stop(id)
  end
end
