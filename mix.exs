defmodule Raft.MixProject do
  use Mix.Project

  def project do
    [
      app: :exraft,
      version: "1.0.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: []
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Raft.Application, []}
    ]
  end
end
