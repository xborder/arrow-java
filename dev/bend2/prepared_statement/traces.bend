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

# The trace generator: enumerates client request sequences over two
# handle ids, runs each through the proven model under both bind
# policies, and prints one line per trace:
#
#   C B0 E0 | ok0 ok1 err | ok0 ok0 ok0
#
# actions, then the expected responses under the rotating policy, then
# under the keeping policy. `okN` carries the handle id the model
# returned, which the Java test uses to map model ids to real handles.
#
#   bend traces.bend > ../../../flight/flight-sql/src/test/resources/bend2/prepared_statement_traces.txt

import Base
import ./main.bend as PS

# Alphabet
# --------

# the actions a trace is built from
def alphabet() -> List<&2, PS.Act>:
  [PS.ACreate{}, PS.ABind{0n}, PS.ABind{1n}, PS.AExec{0n}, PS.AExec{1n}, PS.AClose{0n}, PS.AClose{1n}]

def is_create(a: PS.Act) -> Bool:
  match a:
    case PS.ACreate{}:
      True{}
    case PS.ABind{h}:
      False{}
    case PS.AExec{h}:
      False{}
    case PS.AClose{h}:
      False{}

def is_bind0(a: PS.Act) -> Bool:
  match a:
    case PS.ACreate{}:
      False{}
    case PS.ABind{h}:
      Nat.is_eq(h, 0n)
    case PS.AExec{h}:
      False{}
    case PS.AClose{h}:
      False{}

# Enumeration
# -----------

# t followed by each action of as
def extend_one(+t: List<&2, PS.Act>, as: List<&2, PS.Act>) -> List<&2, List<&2, PS.Act>>:
  match as:
    case Nil{}:
      Nil{}
    case Con{+a, rest}:
      List.append(&2, PS.Act, t, [a]) <> extend_one(t, rest)

# every trace of ts, extended by one action
def extend(ts: List<&2, List<&2, PS.Act>>) -> List<&2, List<&2, PS.Act>>:
  match ts:
    case Nil{}:
      Nil{}
    case Con{+t, rest}:
      List.append(&2, List<&2, PS.Act>, extend_one(t, alphabet()), extend(rest))

# whether a trace starts with a Create
def starts_create(t: List<&2, PS.Act>) -> Bool:
  match t:
    case Nil{}:
      False{}
    case Con{a, rest}:
      is_create(a)

# whether a trace starts with Create, Bind 0
def starts_create_bind(t: List<&2, PS.Act>) -> Bool:
  match t:
    case Nil{}:
      False{}
    case Con{a, rest}:
      match rest:
        case Nil{}:
          False{}
        case Con{b, rest2}:
          is_create(a) && is_bind0(b)

def keep(t: List<&2, PS.Act>, r: List<&2, List<&2, PS.Act>>, hit: Bool) -> List<&2, List<&2, PS.Act>>:
  match hit:
    case True{}:
      t <> r
    case False{}:
      r

def only_create(ts: List<&2, List<&2, PS.Act>>) -> List<&2, List<&2, PS.Act>>:
  match ts:
    case Nil{}:
      Nil{}
    case Con{+t, rest}:
      keep(t, only_create(rest), starts_create(t))

def only_create_bind(ts: List<&2, List<&2, PS.Act>>) -> List<&2, List<&2, PS.Act>>:
  match ts:
    case Nil{}:
      Nil{}
    case Con{+t, rest}:
      keep(t, only_create_bind(rest), starts_create_bind(t))

# all traces of length 1 to 3, the length-4 ones that start with a
# Create, and the length-5 ones that start with Create, Bind 0
def all_traces() -> List<&2, List<&2, PS.Act>>:
  +l1 = extend([Nil{}])
  +l2 = extend(l1)
  +l3 = extend(l2)
  +l4 = extend(only_create(l3))
  +l5 = extend(only_create_bind(l4))
  List.concat(&2, List<&2, PS.Act>, [l1, l2, l3, l4, l5])

# Show
# ----

def act_show(a: PS.Act) -> String:
  match a:
    case PS.ACreate{}:
      "C"
    case PS.ABind{h}:
      "B" ++ Nat.show(h)
    case PS.AExec{h}:
      "E" ++ Nat.show(h)
    case PS.AClose{h}:
      "X" ++ Nat.show(h)

def resp_show(r: PS.Resp) -> String:
  match r:
    case PS.ROk{h}:
      "ok" ++ Nat.show(h)
    case PS.RErr{}:
      "err"

def acts_show(t: List<&2, PS.Act>) -> String:
  match t:
    case Nil{}:
      ""
    case Con{a, rest}:
      match rest:
        case Nil{}:
          act_show(a)
        case Con{b, rest2}:
          act_show(a) ++ " " ++ acts_show(b <> rest2)

def resps_show(rs: List<&2, PS.Resp>) -> String:
  match rs:
    case Nil{}:
      ""
    case Con{r, rest}:
      match rest:
        case Nil{}:
          resp_show(r)
        case Con{r2, rest2}:
          resp_show(r) ++ " " ++ resps_show(r2 <> rest2)

# one line: the actions, then the responses under rotate, then under keep
def line(+t: List<&2, PS.Act>) -> String:
  acts_show(t) ++ " | " ++ resps_show(PS.responses(t, True{})(PS.start())) ++ " | " ++ resps_show(PS.responses(t, False{})(PS.start()))

def lines(ts: List<&2, List<&2, PS.Act>>) -> String:
  match ts:
    case Nil{}:
      ""
    case Con{t, rest}:
      line(t) ++ "\n" ++ lines(rest)

def header() -> String:
  "# Licensed to the Apache Software Foundation (ASF) under one\n"
  ++ "# or more contributor license agreements.  See the NOTICE file\n"
  ++ "# distributed with this work for additional information\n"
  ++ "# regarding copyright ownership.  The ASF licenses this file\n"
  ++ "# to you under the Apache License, Version 2.0 (the\n"
  ++ "# \"License\"); you may not use this file except in compliance\n"
  ++ "# with the License.  You may obtain a copy of the License at\n"
  ++ "#\n"
  ++ "#   http://www.apache.org/licenses/LICENSE-2.0\n"
  ++ "#\n"
  ++ "# Unless required by applicable law or agreed to in writing,\n"
  ++ "# software distributed under the License is distributed on an\n"
  ++ "# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n"
  ++ "# KIND, either express or implied.  See the License for the\n"
  ++ "# specific language governing permissions and limitations\n"
  ++ "# under the License.\n"
  ++ "#\n"
  ++ "# GENERATED by dev/bend2/prepared_statement/traces.bend from the proven\n"
  ++ "# Flight SQL prepared-statement model. Do not edit by hand.\n"
  ++ "# Format: actions | expected responses (rotating bind) | expected responses (keeping bind)\n"
  ++ "# Actions: C = CreatePreparedStatement, Bh = DoPut bind on handle h,\n"
  ++ "#          Eh = GetFlightInfo execute on handle h, Xh = ClosePreparedStatement on h.\n"
  ++ "# Responses: okN = success returning handle id N, err = error status.\n"

def main() -> IO(Unit):
  IO.print(header() ++ lines(all_traces()))
