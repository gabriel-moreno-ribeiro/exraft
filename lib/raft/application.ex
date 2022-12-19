defmodule Raft.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Raft.Registry},
      Raft.Network
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Raft.Supervisor)
  end
end
