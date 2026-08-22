# langgraph-akka

Saves the state of a running graph after every step, and works out what a resumed run
should do again and what it should skip.

A port of [langchain-ai/langgraph](https://github.com/langchain-ai/langgraph) onto **Akka**,
built with **Akka Specify**.

---

## Where it came from

langchain-ai/langgraph is a Python library for building programs out of boxes joined by
arrows, where the program runs one step at a time and can be stopped and picked up again. It
was ported to derive a specification format precise enough to regenerate a system on a
different stack — the port is the vehicle, the specification is the deliverable.

The part rebuilt here is the part that saves and restores: the store a run writes to after
every step, and the rule that decides which work a resumed run repeats. The part not rebuilt
is the runner that executes the boxes.

The specifications the port was generated from are in
[TylerJewell/akka-specify-harness](https://github.com/TylerJewell/akka-specify-harness) under
`langgraph-port/`.

---

## langchain-ai/langgraph → this port

📉 774 Python lines → **445 Java lines**<br>
📁 41 files → **17 files**<br>
⚡ 26,184 nanoseconds → **3,356 nanoseconds** per save-and-read sequence<br>
⚡ 60,238 nanoseconds → **5,009 nanoseconds** per listing sequence<br>
🎯 53 answers matched → **53 answers matched**<br>
🧪 63 tests<br>
💾 keeps every step forever → **keeps the most recent 128 steps of each chain**

Full method and the numbers that did *not* make this list:
[`../langgraph-port/bench/REPORT.md`](https://github.com/TylerJewell/akka-specify-harness/blob/main/langgraph-port/bench/REPORT.md).

---

## What it took to build

⏱️ **4.0 hours** from the first command to the published repository, **1.0** of them active<br>
💬 **322** exchanges with the model<br>
✍️ **277,468** tokens written by the model, **45,072,799** counting everything sent and re-sent<br>
🙋 **0** questions to a human<br>
🧪 **63** tests

The record of every question, and where the time went, is in
[`port-log/`](https://github.com/TylerJewell/akka-specify-harness/tree/main/port-log).

---

## What it does

- **Every step of a run is kept, and each one names the step it came after.** A run's whole
  history can be walked backwards from its most recent step, and any earlier step can be read
  directly.
- **A thread can hold more than one chain.** A run that starts a smaller run inside itself
  gives the smaller one its own name, and the two chains have their own most-recent steps and
  cannot see each other's history.
- **A value is stored once for each version of the box that holds it.** A step that leaves a
  box alone stores no second copy of what is in it, and a later read finds the copy the step
  before it stored.
- **A piece of work that reports its result twice has the first result kept and the second
  dropped.** Four named kinds of report are the exception: a failure, a scheduling note, a
  stop-and-ask, and an answer to one — for those four, the later report replaces the earlier.
- **Reporting a result against a step that was never saved is refused, and nothing is
  written.** The refusal says which step was named.
- **A resumed run repeats the work that did not finish and skips the work that did.** What
  separates them is what each piece of work left behind: a finished one left its results,
  and those are handed back to it instead of running it again.
- **A history that grows past 128 steps in one chain is cut back to its most recent step.**
  The step that is kept keeps its results and its stored values; everything the kept step
  does not name is removed.
- **Deleting a thread removes its steps, its results and its stored values across every one
  of its chains, and touches nothing belonging to any other thread.**

---

## Design decisions

**Event sourcing.** Saving a run's history means never losing a step, so the service writes
down each change as it happens rather than overwriting a running total. Reading the history
back is then just replaying what was written, and a restart loses nothing.

**One thread, one durable object.** Everything a single run needs to stay consistent —
its steps, its results, its stored values — is decided together, so it is all kept together
and changed one instruction at a time. Two runs never wait for each other, because they are
separate objects.

**The chain name lives inside the object, not in its name.** The runtime reserves a character
that a combined name would need, and every character it does accept is one a caller's own
name might contain, so combining the two could not be undone reliably. Keeping the chain name
inside means any name a caller picks works.

**A bound on how much one thread keeps.** The runtime stops copying an object between regions
once it passes a size, and stops the object entirely further up, so a history that grew
forever would quietly stop being safe. Cutting back to the most recent step at 128 keeps every
thread well under both limits.

**Refusals happen at the front door.** A name too long or holding a reserved character reaches
the runtime as a ten-second wait that explains nothing, so the service checks both before it
passes anything on. A caller gets an immediate answer naming the limit it went past.

---

## Running it — the short path

You do not need Java, Maven, or the Akka CLI installed. Akka Specify installs them for you.

**1. Install Akka Specify** in Claude Code:

```
/plugin marketplace add akka/ai-marketplace
/plugin install akka@akka-ai-marketplace
```

Restart Claude Code when it asks.

**2. Give it this prompt:**

> Clone https://github.com/TylerJewell/langgraph-akka into a new directory and open it.
> Then run /akka:setup to install everything this project needs, and /akka:build to
> compile it, run the tests, and start it locally.

**3. Call it** at http://localhost:9055 — see the routes below.

---

## Running it — the developer path

### Requirements

- Java 21 or newer
- Maven 3.9 or newer
- An Akka download token — run `akka code token` once

### Start the service

```bash
mvn compile
akka local run
```

The service starts on **port 9055**.

### The routes

| Call | What it does |
|---|---|
| `POST /threads/{threadId}/checkpoints` | save a step; answers with the identifier to name as the previous step next time |
| `GET /threads/{threadId}/checkpoints/latest` | read the most recent step of the unnamed chain |
| `POST /threads/{threadId}/checkpoints/read` | read a named step, or the most recent one of a named chain |
| `POST /threads/{threadId}/checkpoints/list` | list a chain's history, newest first, with three optional filters |
| `POST /threads/{threadId}/writes` | record what a piece of work produced against a saved step |
| `POST /threads/{threadId}/resume-plan` | ask which of a list of pieces of work to run again and which to skip |
| `POST /threads/{threadId}/prune/{strategy}` | cut a thread back — `keep_latest` or `delete` |
| `DELETE /threads/{threadId}` | remove everything the thread holds |

### Run the tests

```bash
mvn verify
```

63 tests: 46 that need no runtime, and 17 that start one.

---

## Configuration

| Variable | Default | Notes |
|---|---|---|
| none | — | The service calls no model and no outside system, and reads no environment variable of its own. |

The one number worth knowing is not a variable: a chain is cut back to its most recent step
once it passes **128** saved steps. It is a constant in `CheckpointStore` because changing it
changes what the service promises, and that belongs in a release rather than in a deployment.

---

## Where it differs from langchain-ai/langgraph

Everything not listed here behaves the same way on purpose, including the parts that look
like mistakes.

- **How much history is kept.** langgraph keeps every step of a thread for as long as the
  store lives. This port cuts a chain back to its most recent step once it passes 128, keeping
  that step's results and the values it still names. The runtime this port runs on stops
  copying an object between regions past a size and stops the object entirely further up, and
  the second of those reaches a caller as a ten-second wait naming nothing — a bound chosen in
  advance is the only version of that anybody can act on.
- **Nothing invokes the cutting-back automatically in the original.** langgraph's own store
  interface describes two ways of cutting a thread back and its test suite checks seven things
  about them, but no store that ships with it implements either, and nothing in it ever calls
  for one. This port implements both descriptions and calls the first itself, at 128. The
  trigger is this port's own, because there was none to copy.
- **What the version marker looks like past its counter.** Both save a marker made of a
  32-digit counter, a dot, and something after the dot, and both decide which version is newer
  from the counter alone. langgraph puts a random Python decimal number after the dot; this
  port puts a random 16-digit hexadecimal number. Nothing in either reads what is after the
  dot, and no other language reproduces Python's rendering of a decimal number character for
  character.
- **Which of two markers made at the same moment is newer.** langgraph's answer is decided by
  a fresh random number each time, so two boxes changed in the same step compare in an order
  nothing about the system determines. This port compares on the counter alone and returns the
  first of a tie, so the same two markers always compare the same way. Nothing in langgraph
  depends on that comparison, so no behaviour that exists in it is lost.
- **What a caller sees when it asks for something that is not there.** langgraph's store
  returns nothing at all. This port answers `404` over its own routes, and inside the service
  reports absence as an answer rather than as a failure, so that a genuine failure further
  down is not flattened into "not found".
- **Two names a caller cannot use.** A thread name of 240 characters or more, or one holding a
  vertical bar, is refused with `400` and a message naming the limit. langgraph accepts both.
  Passed through, the first is refused by the runtime by name and the second reaches the caller
  as a ten-second wait explaining nothing.
- **Concurrent saves to one thread.** Both keep every save and both list the result
  newest-first — four callers saving at once to one thread lose nothing on either side. What
  differs is why. langgraph's in-memory store gets it from the Python interpreter's own lock
  around a dictionary, which is a property of that one store rather than of the interface it
  implements. This port gets it from handling one instruction per thread at a time, which is
  how the runtime works whatever is underneath. Checked by running it: four callers of fifty
  saves each on the langgraph side, four of twenty-five on this one.
- **What happens across a lost connection.** Neither side has one to lose. langgraph's
  checkpoint library ships no server, no page and no long-lived connection of any kind, and
  this port's only way in is a request that is answered and closed. There is nothing here to
  compare, and it is named because a reader looking for it should find the answer rather than
  silence.
- **Saving a step under an identifier that is already saved.** Both replace what was there,
  and the later save's description wins. Neither system's own runner ever does this, because
  both mint a fresh identifier every time; it is stated because the two agree and a reader has
  no way to know that without being told.
- **Recording results against a step that was never saved.** langgraph accepts and keeps them,
  where they sit unreachable because a read of that step returns nothing. This port refuses
  with `400` and writes nothing. Keeping a record no read can reach is a record that cannot be
  cleaned up either, and the refusal names the step, which is what a caller that got the
  identifier wrong needs.

---

## Licence

langchain-ai/langgraph is MIT, © 2024 LangChain, Inc. This port reimplements the behaviour
without copied source; see
[`ACKNOWLEDGEMENTS.md`](ACKNOWLEDGEMENTS.md).

This project is MIT.
