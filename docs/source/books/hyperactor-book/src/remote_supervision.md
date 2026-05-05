# Remote Supervision and Rendezvous

The `hyperactor_remote` crate provides two related mechanisms:

- remote supervision, which projects a local Hyperactor supervision relationship across a process boundary, and
- rendezvous tokens, which let independently started actors exchange typed actor references through an opaque token.

Use remote supervision when a parent actor must own the lifecycle of a child that runs behind another actor. Use rendezvous tokens when the actors do not yet have direct references to each other, or when a token must cross a boundary outside normal actor messaging.

## Remote supervision

Remote supervision connects one parent-side `Supervisor` actor to one worker-side `Worker<C>` actor. The worker owns and supervises the real child `C`. The supervisor is a proxy child of the parent that re-raises worker-reported lifecycle events through the ordinary `ActorSupervisionEvent` path.

```text
parent proc                                      worker proc

Parent actor
  |
  | spawns
  v
Supervisor  -- Link ------------------------->  Worker<C>
  |                                             |
  | spawns                                      | spawns
  v                                             v
KeepaliveSupervisor <--- keepalive link --->   KeepaliveWorker
                                                |
                                                | supervises
                                                v
                                                Child C
```

The parent sees a conventional local child: the `Supervisor`. The worker sees a conventional local child: `C`. `hyperactor_remote` translates between those local supervision trees with explicit protocol messages.

## Link protocol

A session starts when `Supervisor::init` spawns the supervisor side of the link and sends `Link` to the worker.

```text
Supervisor                                   Worker<C>
    |                                            |
    | spawn KeepaliveSupervisor                 |
    |                                            |
    | Link { session_id, supervisor, parent,    |
    |        link, options }                    |
    |------------------------------------------->|
    |                                            | reject if already linked
    |                                            | spawn link actor from LinkSpec
    |                                            | identify child C
    |                                            |
    | WorkerSupervisor::Linked { child, ... }   |
    |<-------------------------------------------|
    |                                            |
    | session established                        | session established
```

The `session_id` is part of every later command and event. Both sides reject messages for the wrong session. A `Worker<C>` accepts only one active session; a second `Link` receives `WorkerSupervisor::LinkRejected`.

`LinkSpec` describes a registered remote-spawnable actor and its serialized parameters. The worker spawns that actor as a supervised child. The actor's lifecycle is the link lifecycle: if the link actor fails, ordinary Hyperactor supervision reports that failure to the actor that spawned it.

## Keepalive link

`KeepaliveLink` is the default link implementation. It creates a `KeepaliveSupervisor` on the parent side and a `KeepaliveWorker` specification for the worker side.

```text
KeepaliveWorker                              KeepaliveSupervisor
    |                                            |
    | Keepalive { generation, reply }            |
    |------------------------------------------->|
    |                                            |
    | KeepaliveAck { generation }                |
    |<-------------------------------------------|
    |                                            |
    | wait interval, then send next generation   |
```

The worker side fails if an acknowledgment is not received before its timeout. The supervisor side fails if the next keepalive is not delivered before its timeout. Either failure becomes a supervision event for the side that owns the failed link actor.

## Child and link failures

When the child `C` fails or stops, `Worker<C>` reports the event to the supervisor side with `RemoteActorDisposition::Terminal`, clears its child handle, and exits.

```text
Child C fails
    |
    v
Worker<C>::handle_supervision_event
    |
    | WorkerSupervisor::SupervisionEvent {
    |   disposition: Terminal,
    |   event: child event
    | }
    v
Supervisor
    |
    | returns ActorErrorKind::UnhandledSupervisionEvent
    v
Parent actor receives an ordinary supervision event
```

When the worker-side link actor fails, `Worker<C>` reports a synthesized child event with `RemoteActorDisposition::Unreachable`. The child may still be running, because the failure only proves that the supervision channel is broken. The worker then applies its `OrphanPolicy`.

```text
Worker-side link fails
    |
    v
Worker<C>
    |
    | reports child as Unreachable
    | applies OrphanPolicy
    |
    +-- Stop ---------> stop Child C
    |
    +-- LeaveRunning -> keep Child C alive
```

When the parent-side link actor fails, `Supervisor` synthesizes an unreachable supervision event for the remote child if it already knows the child address. If the session was not linked yet, the event names the worker.

## Stop and unlink

Stopping the parent-side `Supervisor` mirrors the stop to the worker. If the stop arrives before the worker has replied with `Linked`, the supervisor records it and sends it once the session is established.

```text
Parent stops Supervisor
    |
    v
Supervisor::handle_stop
    |
    | SupervisedWorker::Stop { session_id, mode, reason }
    v
Worker<C>
    |
    v
Child C receives stop or drain-and-stop
```

`SupervisedWorker::Unlink` tears down the session without treating it as a child failure. The worker stops the link actor, applies the configured orphan policy, and replies with `WorkerSupervisor::Unlinked`.

## Rendezvous tokens

A token is a serializable capability that points at a supervised rendezvous actor. The token creator provides its own typed reference and a private port for join notifications. Token holders call `Token::join` with their typed reference and a result port.

```text
Creator actor
    |
    | create(this, creator_ref, creator_joined, options)
    v
Rendezvous<C, J> child
    |
    | Token<C, J> serializes the rendezvous ActorRef
    v
external channel: CLI, file, config, scheduler metadata, or actor message
    |
    v
Joiner actor
    |
    | token.join(joiner_ref, result)
    v
Rendezvous<C, J>
    |
    +--> creator_joined receives Joined<J> { peer: joiner_ref }
    |
    +--> joiner result receives JoinResult<C>::Joined { peer: creator_ref }
```

The token is typed by the creator peer type `C` and joiner peer type `J`. Its serialized payload includes the type URIs for `C`, `J`, and the rendezvous behavior. Deserialization rejects tokens used with incompatible peer types.

`TokenPolicy::Multi` accepts every join while the rendezvous actor is alive. `TokenPolicy::Once` accepts the first join and rejects later joins with `JoinResult::Rejected`.

## Intended usage

Remote supervision is for ownership. The parent should treat the `Supervisor` as the owned child and let failures propagate through normal supervision. The worker should wrap exactly one local child in `Worker<C>` and allow only the active session to control that child.

Rendezvous tokens are for discovery. They exchange typed references, not ownership. The rendezvous actor is a child of the token creator, so the token remains useful only while the creator and its rendezvous child remain alive.

Use `OrphanPolicy::Stop` when the remote child must not outlive its supervisor. Use `OrphanPolicy::LeaveRunning` only when the child has an independent lifecycle and losing the supervision channel should not stop useful work.

## Public surface

The crate exports these main types:

- `Supervisor`, the parent-side proxy actor.
- `Worker<C>` and `WorkerLike`, the worker-side container and behavior.
- `Link`, `SupervisedWorker`, and `WorkerSupervisor`, the protocol messages.
- `LinkOptions`, `OrphanPolicy`, and `RemoteActorDisposition`, the session policy and event metadata.
- `LinkSpec`, the spawn specification for a link implementation actor.
- `KeepaliveLink`, `KeepaliveSupervisor`, and `KeepaliveWorker`, the built-in message-based link.
- `Token<C, J>`, `TokenPeer`, `Join`, `Joined`, `JoinResult`, `TokenOptions`, and `TokenPolicy`, the typed rendezvous API.
