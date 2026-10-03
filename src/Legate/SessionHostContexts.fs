// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Threading
open System.Threading.Tasks
open Akka.Actor

/// References to host-owned services. This holder never acquires disposal ownership.
type internal SessionExecutionContext =
    {
        Tenant: TenantId
        Store: ISessionStore
        EventStore: ISessionEventStore
        SubscriptionOptions: SessionSubscriptionOptions
        SubscriptionLifetime: SessionSubscriptionLifetime
        WorkTracker: ExecutionWorkTracker
        Spawn: SessionAddress -> IActorContext -> string -> IActorRef
        Client: Lazy<obj>
    }

type internal ISessionHostContexts =
    abstract InitializeAsync: CancellationToken -> Task
    abstract HasDeclaredBindings: bool
    abstract DefaultTenant: TenantId
    abstract Get: TenantId -> SessionExecutionContext
    abstract All: SessionExecutionContext array
    abstract OpenAdmission: unit -> unit
    abstract CloseAdmission: unit -> unit
    abstract DrainAsync: TimeSpan * TimeProvider * CancellationToken -> Task
