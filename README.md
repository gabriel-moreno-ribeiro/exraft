# exraft

> 🇺🇸 [English version below](#english)

O algoritmo de consenso Raft, em Elixir, replicando um key-value store entre um cluster de GenServers. Eleição de líder, replicação de log, regra de commit, follower atrasado alcançando o líder, líder que perde a maioria abdicando, e uma camada de rede com injeção de falhas pra os testes poderem derrubar nós e particionar o cluster.

Raft é aquele paper que todo mundo diz que é "fácil de entender". É, até você implementar. O que me pegou de verdade não foi a eleição, foi a regra 5.4.2: o líder só pode commitar entradas do próprio termo, e sem isso o teste de "líder isolado volta e não sobrescreve nada" fica vermelho de um jeito bem sutil.

```elixir
ids = Raft.Cluster.start(["a", "b", "c"])
{:ok, leader} = Raft.Cluster.wait_for_leader(ids)

{:ok, index} = Raft.Server.command(leader, {:put, :answer, 42})   # responde quando commitou
{:ok, 42} = Raft.Server.read(leader, :answer)
{:error, {:not_leader, "a"}} = Raft.Server.command("b", {:put, :x, 1})

Raft.Network.isolate(leader)                # particiona o líder
{:ok, new_leader} = Raft.Cluster.wait_for_leader(ids -- [leader])
Raft.Network.heal()                         # o líder antigo abdica e se atualiza
```

```sh
mix test
```

## O que acontece

- **Papéis e termos** (`Raft.Server`): todo servidor nasce follower com um timeout de eleição aleatório (150 a 300 ms). Sem heartbeat, vira candidato, incrementa o termo, vota em si e manda `request_vote`. O voto é dado uma vez por termo e só pra quem tem log pelo menos tão atualizado. Maioria vira líder, que manda `append_entries` a cada 50 ms. Qualquer mensagem com termo maior faz o servidor abdicar.
- **Replicação**: o comando entra no log do líder e vai pros followers com o índice e o termo da entrada anterior. Follower rejeita quando isso não bate, o líder recua o `next_index` daquele follower e tenta de novo, então logs divergentes são consertados a partir do último ponto em comum. Entradas conflitantes do follower são truncadas.
- **Commit**: o líder commita um índice quando a maioria tem ele, restrito a entradas do próprio termo. Entradas commitadas são aplicadas em ordem no store e o cliente que estava esperando recebe `{:ok, index}`. Followers descobrem o commit index pelos heartbeats.
- **Leituras** só pelo líder, pra nunca ler valor velho de um follower.
- **Rede** (`Raft.Network`): servidores registrados num `Registry` e todo RPC passa por uma função que pode descartar mensagens de nós isolados ou cortar links.

Elixir foi a linguagem certa pra isso: cada servidor é um processo, cada RPC é uma mensagem, e o "cluster de três nós" dos testes sobe e cai dentro de um único BEAM em milissegundos.

Testes (ExUnit, clusters reais de três nós): exatamente um líder; comandos replicam e todos os stores convergem; followers redirecionam; o cluster sobrevive à queda do líder e mantém os dados commitados; líder isolado não commita (sem split brain) e abdica ao voltar; logs iguais depois da partição sarar; termos monotônicos; um nó sozinho commita.

---

## English

The Raft consensus algorithm, in Elixir, replicating a key-value store across a cluster of GenServers. Leader election, log replication, the commit rule, a lagging follower catching up with the leader, a leader that loses the majority stepping down, and a network layer with fault injection so the tests can take nodes down and partition the cluster.

Raft is that paper everybody says is "easy to understand". It is, until you implement it. What really got me wasn't the election, it was rule 5.4.2: the leader can only commit entries from its own term, and without that the "isolated leader comes back and doesn't overwrite anything" test goes red in a pretty subtle way.

```elixir
ids = Raft.Cluster.start(["a", "b", "c"])
{:ok, leader} = Raft.Cluster.wait_for_leader(ids)

{:ok, index} = Raft.Server.command(leader, {:put, :answer, 42})   # replies once committed
{:ok, 42} = Raft.Server.read(leader, :answer)
{:error, {:not_leader, "a"}} = Raft.Server.command("b", {:put, :x, 1})

Raft.Network.isolate(leader)                # partitions the leader
{:ok, new_leader} = Raft.Cluster.wait_for_leader(ids -- [leader])
Raft.Network.heal()                         # the old leader steps down and catches up
```

```sh
mix test
```

## What happens

- **Roles and terms** (`Raft.Server`): every server is born a follower with a random election timeout (150 to 300 ms). With no heartbeat, it becomes a candidate, increments the term, votes for itself and sends `request_vote`. The vote is given once per term and only to whoever has a log at least as up to date. A majority becomes leader, which sends `append_entries` every 50 ms. Any message with a higher term makes the server step down.
- **Replication**: the command goes into the leader's log and out to the followers with the index and term of the previous entry. A follower rejects when that doesn't match, the leader backs off that follower's `next_index` and tries again, so diverging logs get repaired from the last common point. Conflicting follower entries are truncated.
- **Commit**: the leader commits an index when a majority has it, restricted to entries from its own term. Committed entries are applied in order to the store and the waiting client gets `{:ok, index}`. Followers learn the commit index through the heartbeats.
- **Reads** only through the leader, so you never read a stale value from a follower.
- **Network** (`Raft.Network`): servers registered in a `Registry` and every RPC goes through a function that can drop messages from isolated nodes or cut links.

Elixir was the right language for this: every server is a process, every RPC is a message, and the tests' "three-node cluster" comes up and goes down inside a single BEAM in milliseconds.

Tests (ExUnit, real three-node clusters): exactly one leader; commands replicate and all the stores converge; followers redirect; the cluster survives the leader going down and keeps the committed data; an isolated leader doesn't commit (no split brain) and steps down when it comes back; logs are equal after the partition heals; monotonic terms; a single node commits on its own.

MIT.
