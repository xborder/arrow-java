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

# The laws of the prepared-statement model. The human states them;
# PROOF.bend must prove them. Each law is a sentence from the Flight
# SQL spec, or a sanity check that keeps the others from being
# satisfied vacuously.

import Base
import ./main.bend as PS

# the truth of a Bool as a type: Unit when True, Empty when False
def T(b: Bool) -> Data:
  match b:
    case True{}:
      Unit
    case False{}:
      Empty

# every live handle is below the counter, so no handle is ever reissued
def Fresh(live: List<&2, Nat>, +next: Nat) -> Data:
  T(PS.all_lt(live, next))

def Fresh_s(s: PS.Server) -> Data:
  PS.Server{live, next} = s
  Fresh(live, next)

# LAW: "Closes server resources associated with the prepared statement
# handle." Once a handle is closed, executing it is an error, from any
# server state and under either bind policy.
law closed_never_executes:
  for +h    : Nat
  for +live : List<&2, Nat>
  for +next : Nat
  for  rot  : Bool
  {PS.resp(PS.step_s(PS.AExec{h}, PS.state(PS.step(PS.AClose{h}, live, next, rot)), rot)) == PS.RErr{} : PS.Resp}

# LAW (anti-vacuity): a server that rejects everything satisfies the law
# above. This one rules that out: the handle a Create returns executes.
law created_handle_executes:
  for +live : List<&2, Nat>
  for +next : Nat
  for  rot  : Bool
  {PS.resp(PS.step_s(PS.AExec{next}, PS.state(PS.step(PS.ACreate{}, live, next, rot)), rot)) == PS.ROk{next} : PS.Resp}

# LAW: "The server is responsible for detecting the case where the
# client does not use the updated handle and should return an error."
# Under the rotating policy, after a bind rotates h, executing the
# stale h is an error. This needs the freshness invariant, so it is
# stated for fresh states only, and the two laws below show every
# reachable state is fresh.
law stale_handle_rejected:
  for +h    : Nat
  for +live : List<&2, Nat>
  for +next : Nat
  for  w    : Fresh(live, next)
  {PS.resp(PS.step_s(PS.AExec{h}, PS.state(PS.step(PS.ABind{h}, live, next, True{})), True{})) == PS.RErr{} : PS.Resp}

# LAW: a server that does not return an updated handle keeps the bound
# handle valid: under the keeping policy, after a bind of a live h,
# executing h succeeds.
law kept_handle_executes:
  for +h    : Nat
  for +live : List<&2, Nat>
  for +next : Nat
  for  hw   : T(PS.has(h, live))
  {PS.resp(PS.step_s(PS.AExec{h}, PS.state(PS.step(PS.ABind{h}, live, next, False{})), False{})) == PS.ROk{h} : PS.Resp}

# LAW: the initial state is fresh
law fresh_start:
  Fresh_s(PS.start())

# LAW: every request keeps the state fresh, under either policy
law fresh_kept:
  for  a    : PS.Act
  for +live : List<&2, Nat>
  for +next : Nat
  for  rot  : Bool
  for  w    : Fresh(live, next)
  Fresh_s(PS.state(PS.step(a, live, next, rot)))
