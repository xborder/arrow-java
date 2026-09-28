<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Flight SQL prepared statements: `CommandPreparedStatementQuery` and `CommandPreparedStatementUpdate`

This document describes how the two prepared-statement *execution* commands of
the Flight SQL protocol map onto the underlying Flight RPCs (`DoAction`,
`DoPut`, `GetFlightInfo`, `DoGet`), and how the Java client and server
implementations in this repository drive them.

The diagrams are written in [Mermaid](https://mermaid.js.org/) and render
directly on GitHub.

Sources of truth used for this document:

- Protocol definition: [`arrow-format/FlightSql.proto`](../../../arrow-format/FlightSql.proto)
- Client: [`FlightSqlClient.PreparedStatement`](../src/main/java/org/apache/arrow/flight/sql/FlightSqlClient.java)
- Server dispatch: [`FlightSqlProducer`](../src/main/java/org/apache/arrow/flight/sql/FlightSqlProducer.java)
- Reference servers: [`FlightSqlExample`](../src/test/java/org/apache/arrow/flight/sql/example/FlightSqlExample.java)
  (stateful) and [`FlightSqlStatelessExample`](../src/test/java/org/apache/arrow/flight/sql/example/FlightSqlStatelessExample.java)
  (stateless)
- JDBC driver: [`ArrowFlightSqlClientHandler`](../../flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/client/ArrowFlightSqlClientHandler.java)
  and [`ArrowFlightMetaImpl`](../../flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightMetaImpl.java)

## 1. Messages, RPCs and Java entry points

Every Flight SQL request is a protobuf message wrapped in a
`google.protobuf.Any`, carried either in the `body` of an `Action`
(`DoAction`), in the `cmd` of a `FlightDescriptor` (`GetFlightInfo`,
`GetSchema`, `DoPut`) or in the bytes of a `Ticket` (`DoGet`).
`FlightSqlProducer` unpacks the `Any`, inspects its type URL and dispatches
to a type-specific method.

| Protobuf message | Carried in | Flight RPC | Client call (`FlightSqlClient`) | Server method (`FlightSqlProducer`) |
| --- | --- | --- | --- | --- |
| `ActionCreatePreparedStatementRequest` | `Action.body` | `DoAction("CreatePreparedStatement")` | `prepare(query)` | `createPreparedStatement(...)` |
| `ActionCreatePreparedStatementResult` | `Result.body` | reply to the action above | `PreparedStatement` constructor | (returned by the server) |
| `CommandPreparedStatementQuery` | `FlightDescriptor.cmd` | `DoPut` | `PreparedStatement.execute()` when parameters are bound | `acceptPutPreparedStatementQuery(...)` |
| `CommandPreparedStatementQuery` | `FlightDescriptor.cmd` | `GetFlightInfo` | `PreparedStatement.execute()` | `getFlightInfoPreparedStatement(...)` |
| `CommandPreparedStatementQuery` | `FlightDescriptor.cmd` | `GetSchema` | `PreparedStatement.fetchSchema()` | `getSchemaPreparedStatement(...)` |
| `CommandPreparedStatementQuery` | `Ticket` (server's choice) | `DoGet` | `getStream(ticket)` | `getStreamPreparedStatement(...)` |
| `DoPutPreparedStatementResult` | `PutResult.app_metadata` | reply to `DoPut` | parsed inside `execute()` | (optionally returned by the server) |
| `CommandPreparedStatementUpdate` | `FlightDescriptor.cmd` | `DoPut` | `PreparedStatement.executeUpdate()` | `acceptPutPreparedStatementUpdate(...)` |
| `DoPutUpdateResult` | `PutResult.app_metadata` | reply to `DoPut` | parsed inside `executeUpdate()` | (returned by the server) |
| `ActionClosePreparedStatementRequest` | `Action.body` | `DoAction("ClosePreparedStatement")` | `PreparedStatement.close()` | `closePreparedStatement(...)` |

The essential difference between the two commands:

- **`CommandPreparedStatementQuery`** is a *two phase* protocol. `DoPut`
  only **binds** parameter values to the statement; it does not run the query.
  `GetFlightInfo` **executes** (or schedules) the statement and returns the
  endpoints and tickets from which the result set is read with `DoGet`.
- **`CommandPreparedStatementUpdate`** is a *single phase* protocol. The
  `DoPut` call both binds parameters and **executes** the update. The affected
  row count travels back in the `PutResult` acknowledgement as a
  `DoPutUpdateResult`. `GetFlightInfo` is never used for updates.

## 2. Lifecycle overview

```mermaid
sequenceDiagram
    autonumber
    participant App as Application
    participant C as FlightSqlClient
    participant S as FlightSqlProducer (server)

    App->>C: prepare("SELECT ... WHERE id = ?")
    C->>S: DoAction CreatePreparedStatement(ActionCreatePreparedStatementRequest{query, transaction_id?})
    S-->>C: Result(ActionCreatePreparedStatementResult{handle, dataset_schema, parameter_schema, is_update?})
    C-->>App: PreparedStatement(handle)

    App->>C: setParameters(VectorSchemaRoot)
    Note over C: Held locally, nothing is sent yet

    alt is_update == false (or result-set query)
        App->>C: execute()
        C->>S: DoPut(cmd=CommandPreparedStatementQuery{handle}, parameter batch)
        S-->>C: PutResult(DoPutPreparedStatementResult{handle'})  (optional)
        C->>S: GetFlightInfo(cmd=CommandPreparedStatementQuery{handle or handle'})
        S-->>C: FlightInfo{schema, endpoints[ticket...]}
        C-->>App: FlightInfo
        App->>C: getStream(ticket)
        C->>S: DoGet(ticket)
        S-->>C: FlightData stream (result set)
    else is_update == true (DML)
        App->>C: executeUpdate()
        C->>S: DoPut(cmd=CommandPreparedStatementUpdate{handle}, parameter batch or empty batch)
        S-->>C: PutResult(DoPutUpdateResult{record_count})
        C-->>App: record_count
    end

    App->>C: close()
    C->>S: DoAction ClosePreparedStatement(ActionClosePreparedStatementRequest{handle})
    S-->>C: (empty result stream)
```

Steps 1 to 4 happen once per prepared statement. Steps 5 onward can be
repeated: the application may rebind parameters and execute the same handle
many times before closing it.

## 3. `CommandPreparedStatementQuery` without bound parameters

When `execute()` is called and no parameter root was set (or the root has zero
rows), the client skips `DoPut` entirely and goes straight to `GetFlightInfo`.

```mermaid
sequenceDiagram
    autonumber
    participant App as Application
    participant PS as FlightSqlClient.PreparedStatement
    participant FC as FlightClient (gRPC)
    participant FS as FlightService (gRPC server)
    participant P as FlightSqlProducer
    participant Impl as Producer implementation

    App->>PS: execute()
    PS->>PS: checkOpen()
    PS->>PS: descriptor = command(Any.pack(CommandPreparedStatementQuery{handle}))
    Note over PS: parameterBindingRoot is null or has 0 rows, so no DoPut

    PS->>FC: getInfo(descriptor)
    FC->>FS: GetFlightInfo(FlightDescriptor)
    FS->>P: getFlightInfo(context, descriptor)
    P->>P: Any.parse(descriptor.cmd) and check type URL
    P->>Impl: getFlightInfoPreparedStatement(CommandPreparedStatementQuery, context, descriptor)
    Note over Impl: Look up the statement by handle,<br/>derive the result schema,<br/>build one or more endpoints with tickets
    Impl-->>P: FlightInfo{schema, endpoints}
    P-->>FS: FlightInfo
    FS-->>FC: FlightInfo
    FC-->>PS: FlightInfo
    PS-->>App: FlightInfo

    loop for each FlightEndpoint in FlightInfo
        App->>FC: getStream(endpoint.ticket)
        FC->>FS: DoGet(Ticket)
        FS->>P: getStream(context, ticket, listener)
        P->>P: Any.parse(ticket.bytes) and check type URL
        P->>Impl: getStreamPreparedStatement(CommandPreparedStatementQuery, context, listener)
        Note over Impl: Execute the statement and stream record batches
        Impl-->>FC: listener.start(root), putNext()..., completed()
        FC-->>App: FlightStream
    end
```

Notes on the server side:

- The ticket format is entirely the server's choice. The reference
  `FlightSqlExample` packs the very same `CommandPreparedStatementQuery` into
  the ticket (`getFlightInfoForSchema`), which is why `FlightSqlProducer.getStream`
  also dispatches on `CommandPreparedStatementQuery`.
- In `FlightSqlExample`, `getFlightInfoPreparedStatement` only reads
  `ResultSetMetaData` to compute the schema. The SQL is actually executed in
  `getStreamPreparedStatement` (`statement.executeQuery()`), i.e. during
  `DoGet`. Other servers may execute during `GetFlightInfo` instead. The
  protocol only says `GetFlightInfo` "executes the prepared statement instance".

## 4. `CommandPreparedStatementQuery` with bound parameters

When the client has a parameter root with at least one row, `execute()` first
uploads it with `DoPut`, then optionally adopts an updated handle, then calls
`GetFlightInfo`.

```mermaid
sequenceDiagram
    autonumber
    participant App as Application
    participant PS as FlightSqlClient.PreparedStatement
    participant FC as FlightClient (gRPC)
    participant FS as FlightService (gRPC server)
    participant P as FlightSqlProducer
    participant Impl as Producer implementation

    App->>PS: setParameters(root)
    App->>PS: execute()
    PS->>PS: descriptor = command(Any.pack(CommandPreparedStatementQuery{handle}))

    rect rgb(235, 245, 255)
        Note over PS,Impl: Phase 1: bind parameters with DoPut
        PS->>FC: startPut(descriptor, root, SyncPutListener)
        FC->>FS: DoPut stream opened, first FlightData carries descriptor + schema
        FS->>P: acceptPut(context, flightStream, ackStream)
        P->>P: Any.parse(descriptor.cmd) and check type URL
        P->>Impl: acceptPutPreparedStatementQuery(CommandPreparedStatementQuery, ...)
        Impl-->>P: Runnable
        Note over FS: Runnable is run on the server executor
        PS->>FC: putNext()  (send the parameter record batch)
        FC->>FS: FlightData(record batch)
        PS->>FC: completed()
        FC->>FS: half-close client stream
        Impl->>Impl: while flightStream.next(): bind each row to the statement
        opt server returns an updated handle (stateless servers)
            Impl-->>FC: ackStream.onNext(PutResult{app_metadata = DoPutPreparedStatementResult{handle'}})
        end
        Impl-->>FC: ackStream.onCompleted()
        PS->>FC: getResult()  (blocks until the server completes the call)
    end

    rect rgb(245, 255, 235)
        Note over PS: Phase 1b: adopt updated handle if one was returned
        alt parameter schema has fields
            PS->>PS: read = putListener.read()
            alt read != null and DoPutPreparedStatementResult.handle non-empty
                PS->>PS: handle = handle'
                PS->>PS: rebuild descriptor with handle'
            else legacy server sent no PutResult
                Note over PS: keep the original handle
            end
        end
    end

    rect rgb(255, 245, 235)
        Note over PS,Impl: Phase 2: execute with GetFlightInfo
        PS->>FC: getInfo(descriptor)
        FC->>FS: GetFlightInfo(FlightDescriptor{cmd=CommandPreparedStatementQuery{handle}})
        FS->>P: getFlightInfo(context, descriptor)
        P->>Impl: getFlightInfoPreparedStatement(...)
        Impl-->>PS: FlightInfo{schema, endpoints}
        PS-->>App: FlightInfo
    end

    App->>FC: getStream(ticket)  for each endpoint
    FC->>FS: DoGet(Ticket)
    FS->>P: getStream(...)
    P->>Impl: getStreamPreparedStatement(CommandPreparedStatementQuery, ...)
    Impl-->>App: result set as FlightData stream
```

Key behaviours to be aware of:

- **The protocol says nothing runs during `DoPut` for a query.** The proto
  comment reads: "DoPut: bind parameter values. All of the bound parameter
  sets will be executed as a single atomic execution." `FlightSqlExample`
  follows this literally: `acceptPutPreparedStatementQuery` binds the rows and
  returns without calling `execute()`, leaving that to `getStreamPreparedStatement`.
- **`DoPutPreparedStatementResult` is optional.** Older servers, and
  `FlightSqlExample` itself, send no `PutResult` at all. The client handles
  this: `putListener.read()` returns `null` when the server completed without
  sending metadata, and the original handle is kept. Only a non-empty
  `prepared_statement_handle` in the result replaces the client's handle.
- **Once the handle is replaced, every later call uses the new one.** This
  includes `GetFlightInfo`, `GetSchema` and `ClosePreparedStatement`. The
  test `testPreparedStatementUsesUpdatedHandleAfterDoPut` in `TestFlightSql`
  asserts exactly this.
- **The client only reads the `PutResult` if the parameter schema has at least
  one field.** If the server reported an empty parameter schema but the
  application still bound rows, the `DoPut` happens but any returned metadata
  is ignored.
- **`putParameters` sends exactly one record batch.** The whole
  `VectorSchemaRoot` goes out as a single `putNext()`. A server that supports
  multiple parameter sets sees them as multiple rows in one batch, not as
  multiple batches.

## 5. Stateless server variant (`FlightSqlStatelessExample`)

The optional updated handle exists so that a server can avoid keeping any
per-statement state between RPCs. `FlightSqlStatelessExample` shows the
pattern: the handle *is* the state.

```mermaid
sequenceDiagram
    autonumber
    participant C as FlightSqlClient.PreparedStatement
    participant S as FlightSqlStatelessExample

    C->>S: DoAction CreatePreparedStatement{query}
    Note over S: handle = UTF-8 bytes of the SQL text (no cache entry needed)
    S-->>C: ActionCreatePreparedStatementResult{handle = query bytes, schemas}

    C->>S: DoPut(cmd=CommandPreparedStatementQuery{handle = query bytes}, parameter batch)
    Note over S: Serialize the parameter batch to an Arrow IPC file,<br/>wrap {query, parameters} into a new opaque handle'
    S-->>C: PutResult(DoPutPreparedStatementResult{handle'})
    C->>C: handle = handle'

    C->>S: GetFlightInfo(cmd=CommandPreparedStatementQuery{handle'})
    Note over S: Decode handle' to recover the query, compute schema
    S-->>C: FlightInfo{ticket = Any.pack(CommandPreparedStatementQuery{handle'})}

    C->>S: DoGet(ticket)
    Note over S: Decode handle' to recover query and parameters,<br/>re-prepare, bind, executeQuery(), stream results
    S-->>C: FlightData stream

    C->>S: DoAction ClosePreparedStatement{handle'}
    S-->>C: (nothing to release)
```

The stateful `FlightSqlExample` instead keeps a Guava cache keyed by handle
holding an open JDBC `PreparedStatement` and `Connection`. Parameters bound
during `DoPut` stay bound on that JDBC object until the next `DoPut` or until
the cache entry is closed.

## 6. `CommandPreparedStatementUpdate`

Updates are a single `DoPut` round trip. There is no `GetFlightInfo` and no
`DoGet`.

```mermaid
sequenceDiagram
    autonumber
    participant App as Application
    participant PS as FlightSqlClient.PreparedStatement
    participant FC as FlightClient (gRPC)
    participant FS as FlightService (gRPC server)
    participant P as FlightSqlProducer
    participant Impl as Producer implementation

    opt parameters present
        App->>PS: setParameters(root)
    end
    App->>PS: executeUpdate()
    PS->>PS: checkOpen()
    PS->>PS: descriptor = command(Any.pack(CommandPreparedStatementUpdate{handle}))
    PS->>PS: if no root: setParameters(VectorSchemaRoot.of())  (empty batch, 0 columns, 0 rows)

    PS->>FC: startPut(descriptor, root, SyncPutListener)
    FC->>FS: DoPut stream opened, first FlightData carries descriptor + schema
    FS->>P: acceptPut(context, flightStream, ackStream)
    P->>P: Any.parse(descriptor.cmd) and check type URL
    P->>Impl: acceptPutPreparedStatementUpdate(CommandPreparedStatementUpdate, ...)
    Impl-->>P: Runnable
    Note over FS: Runnable is run on the server executor

    PS->>FC: putNext()
    FC->>FS: FlightData(parameter batch, possibly 0 rows)
    PS->>FC: completed()
    FC->>FS: half-close client stream

    loop while flightStream.next()
        alt batch has 0 rows
            Impl->>Impl: execute() once, recordCount = getUpdateCount()
        else batch has N rows
            Impl->>Impl: bind row, addBatch() for each row, executeBatch(), sum counts
        end
        Impl-->>FC: ackStream.onNext(PutResult{app_metadata = DoPutUpdateResult{record_count}})
    end
    Impl-->>FC: ackStream.onCompleted()
    PS->>FC: getResult()  (blocks until the server completes)

    PS->>PS: read = putListener.read()
    PS->>PS: DoPutUpdateResult.parseFrom(read.app_metadata)
    PS-->>App: record_count
```

Key behaviours to be aware of:

- **A batch is always sent, even with no parameters.** `executeUpdate()`
  substitutes an empty `VectorSchemaRoot` so the server always sees exactly one
  `flightStream.next()` iteration. `FlightSqlExample` treats a zero-row batch as
  "execute once without binding", which is how parameterless updates such as
  `DELETE FROM t` work.
- **One `PutResult` per incoming batch.** The reference server acknowledges
  every record batch with its own `DoPutUpdateResult`. The Java client sends a
  single batch and reads a single result, so `record_count` is the total for
  that batch. `-1` means unknown.
- **The `PutResult` is mandatory here.** Unlike the query case, the client
  calls `read.getApplicationMetadata()` without a null check, so a server that
  completes the `DoPut` without sending a `DoPutUpdateResult` causes a client
  failure.
- **Multiple parameter rows are batched server side.** In `FlightSqlExample`
  each row in the batch becomes one JDBC `addBatch()` and the result is the sum
  of `executeBatch()`. The protocol leaves the atomicity of that batch to the
  server.

## 7. Closing the statement

```mermaid
sequenceDiagram
    autonumber
    participant App as Application
    participant PS as FlightSqlClient.PreparedStatement
    participant S as FlightSqlProducer (server)

    App->>PS: close()
    alt already closed
        PS-->>App: return (no RPC)
    else
        PS->>PS: isClosed = true
        PS->>S: DoAction ClosePreparedStatement(ActionClosePreparedStatementRequest{handle})
        Note over S: Release server resources for the handle.<br/>Stateful example: invalidate the cache entry.<br/>Stateless example: no-op.
        S-->>PS: onCompleted() with no Result
        PS->>PS: clearParameters() closes the parameter VectorSchemaRoot
        PS-->>App: return
    end
```

The handle sent here is whichever handle the client currently holds, so if a
`DoPut` replaced it the server receives the updated one.

## 8. How the JDBC driver chooses between the two commands

The JDBC driver wraps `FlightSqlClient.PreparedStatement` and decides between
`execute()` (query) and `executeUpdate()` (update) as follows:

```mermaid
flowchart TD
    A[ActionCreatePreparedStatementResult] --> B{is_update set?}
    B -- true --> U[StatementType.UPDATE]
    B -- false --> Q[StatementType.SELECT]
    B -- not set --> C{dataset_schema has fields?}
    C -- no --> U
    C -- yes --> Q
    U --> U2["executeUpdate() → DoPut(CommandPreparedStatementUpdate)"]
    Q --> Q2["execute() → [DoPut(CommandPreparedStatementQuery)] + GetFlightInfo + DoGet"]
```

- `ArrowFlightSqlClientHandler.prepare` implements the decision in
  `getType()`: prefer the server's `is_update`, fall back to "empty result set
  schema means update".
- `ArrowFlightMetaImpl.execute` also routes to `executeUpdate()` when Avatica
  reports the statement as DML (`StatementType.IS_DML`).
- `ArrowFlightMetaImpl.prepareAndExecute` calls `executeUpdate()` immediately
  for `UPDATE` types and defers query execution to the result set, which later
  calls `execute()` and `getStreams(flightInfo)`.

## 9. Quick reference: who does what

| Concern | `CommandPreparedStatementQuery` | `CommandPreparedStatementUpdate` |
| --- | --- | --- |
| RPC that binds parameters | `DoPut` (skipped when no rows are bound) | `DoPut` (always) |
| RPC that executes | `GetFlightInfo` (and/or `DoGet`, server's choice) | `DoPut` |
| RPC that returns data | `DoGet`, one per endpoint | none, only a row count |
| `PutResult` payload | optional `DoPutPreparedStatementResult{handle'}` | required `DoPutUpdateResult{record_count}` |
| Client method | `PreparedStatement.execute()` then `getStream(ticket)` | `PreparedStatement.executeUpdate()` |
| Server methods | `acceptPutPreparedStatementQuery`, `getFlightInfoPreparedStatement`, `getStreamPreparedStatement` | `acceptPutPreparedStatementUpdate` |
| Handle may change | yes, via `DoPutPreparedStatementResult` | no |

See [detached-prepared-update-execution.md](detached-prepared-update-execution.md)
for options to separate parameter binding from execution for
`CommandPreparedStatementUpdate` while staying compatible with existing clients.
