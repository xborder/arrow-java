# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# A model of the Arrow Flight SQL prepared-statement lifecycle, written
# in Bend 2 to see what its laws and proofs cost on a protocol shaped
# like Flight SQL. Handles are Nats issued by a counter; the server
# keeps the list of live handles. This is the "stateless" server style
# from the Flight SQL spec: binding parameters (DoPut) may rotate the
# handle, and the client must use the rotated one afterwards.
#
#   bend main.bend         # checks the model and runs the sample trace in main
#   bend PROOF.bend        # checks LAWS.bend against this model

import Base

# Protocol
# --------

# one client request, by Flight SQL RPC
type Act is Data:
  ACreate{}         # DoAction CreatePreparedStatement
  ABind{h: Nat}     # DoPut  CommandPreparedStatementQuery (bind parameters)
  AExec{h: Nat}     # GetFlightInfo CommandPreparedStatementQuery (execute)
  AClose{h: Nat}    # DoAction ClosePreparedStatement

# the server's answer: the handle to use next, or an error status
type Resp is Data:
  ROk{h: Nat}
  RErr{}

# server state: the live handles, and the next handle to issue
type Server is Data:
  Server{live: List<&2, Nat>, next: Nat}

# List helpers
# ------------

# keep x unless drop
def put(x: Nat, r: List<&2, Nat>, drop: Bool) -> List<&2, Nat>:
  match drop:
    case True{}:
      r
    case False{}:
      x <> r

# xs without every occurrence of h
def remove(+h: Nat, xs: List<&2, Nat>) -> List<&2, Nat>:
  match xs:
    case Nil{}:
      Nil{}
    case Con{+x, t}:
      put(x, remove(h, t), Nat.is_eq(x, h))

# whether h occurs in xs
def has(+h: Nat, xs: List<&2, Nat>) -> Bool:
  match xs:
    case Nil{}:
      False{}
    case x <> t:
      Nat.is_eq(x, h) || has(h, t)

# whether every handle in xs is below n
def all_lt(xs: List<&2, Nat>, +n: Nat) -> Bool:
  match xs:
    case Nil{}:
      True{}
    case Con{x, t}:
      Nat.is_lt(x, n) && all_lt(t, n)

# Transitions
# -----------

# Exec: answer Ok on a live handle, Err otherwise; the state is unchanged
def exec(+h: Nat, live: List<&2, Nat>, +next: Nat, ok: Bool) -> Server & Resp:
  match ok:
    case True{}:
      (Server{live, next}, ROk{h})
    case False{}:
      (Server{live, next}, RErr{})

# Bind: on a live handle, retire it and issue a fresh one; else Err
def bind(+h: Nat, +live: List<&2, Nat>, +next: Nat, ok: Bool) -> Server & Resp:
  match ok:
    case True{}:
      (Server{next <> remove(h, live), 1n+next}, ROk{next})
    case False{}:
      (Server{live, next}, RErr{})

# one request against one state
def step(a: Act, +live: List<&2, Nat>, +next: Nat) -> Server & Resp:
  match a:
    case ACreate{}:
      (Server{next <> live, 1n+next}, ROk{next})
    case ABind{+h}:
      bind(h, live, next, has(h, live))
    case AExec{+h}:
      exec(h, live, next, has(h, live))
    case AClose{+h}:
      (Server{remove(h, live), next}, ROk{h})

# projections of a step
def state(sr: Server & Resp) -> Server:
  (s, r) = sr
  s

def resp(sr: Server & Resp) -> Resp:
  (s, r) = sr
  r

# a request against a whole server
def step_s(a: Act, s: Server) -> Server & Resp:
  Server{live, next} = s
  step(a, live, next)

def start() -> Server:
  Server{Nil{}, 0n}

# a whole trace, threading the state and the last response
def run(acts: List<Act>, sr: Server & Resp) -> Server & Resp:
  match acts:
    case Nil{}:
      sr
    case a <> rest:
      (s, last) = sr
      run(rest, step_s(a, s))

def replay(acts: List<Act>) -> Server & Resp:
  run(acts, (start(), RErr{}))

# Show
# ----

def Resp.show(r: Resp) -> String:
  match r:
    case ROk{h}:
      "Ok(" ++ Nat.show(h) ++ ")"
    case RErr{}:
      "Err"

# create, bind (handle 0 -> 1), execute the stale 0: Err
def main() -> IO(Unit):
  IO.print(Resp.show(resp(replay([ACreate{}, ABind{0n}, AExec{0n}]))))
