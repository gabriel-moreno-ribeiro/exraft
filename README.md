# exraft

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

**EN:** the Raft consensus algorithm in Elixir replicating a key-value store across a cluster of GenServers: randomized elections, log replication with next_index backoff and truncation, the current-term commit rule, leader-only reads, and a fault-injecting network layer used by the ExUnit suite to crash nodes and partition the cluster. MIT.
