# Acknowledgements

This project is a port of **[langchain-ai/langgraph](https://github.com/langchain-ai/langgraph)**.

## Licence and copyright

`langchain-ai/langgraph` is under the **MIT License**, copyright **(c) 2024 LangChain, Inc.**
Read from `langgraph-src/LICENSE` in the clone this port was written against, not from the
repository's badge.

MIT permits use, copying and modification with attribution, and requires the copyright notice
and permission notice to travel with substantial portions of the software. This file is that
attribution; `langgraph-akka/LICENSE` carries the notice.

## Was anything copied verbatim?

**No source file, no test, no fixture and no documentation text was copied.** Every Java file
in `langgraph-akka` was written for this port. What was copied is the *behaviour*, which is
what a port is, and it is copied from a specification (`langgraph-port/specs/SPEC-001-langgraph.md`)
written from the answers recorded in `langgraph-port/docs/question-log.md` — every one of which
was established by running langgraph rather than by reading it.

`python toolkit/copied_strings.py langgraph --source langgraph-src` finds seven string literals
of ten characters or more that occur in both trees. A sentence about each:

| Literal | Where | Why it is in both |
|---|---|---|
| `__interrupt__` | `ControlChannels.java` | **Copied deliberately, and it has to be.** These four names are langgraph's own protocol: a caller that already speaks to a langgraph graph writes them, and a store that spelled them differently would not recognise a write the graph makes. Question-log row 3 enumerates all four and their fixed indices. Reproducing an identifier so that two systems interoperate is the same reason a port of an HTTP server contains the word `Content-Type`. |
| `__resume__` | `ControlChannels.java` | as above |
| `__scheduled__` | `ControlChannels.java` | as above |
| `__error_source_node__` | `ControlChannels.java` | as above — named by the pregel loop alongside `__error__`, and not itself indexed |
| `/checkpoints` | `CheckpointEndpointIntegrationTest.java` | Coincidence, not copying. This is a path segment in the port's own HTTP route, `POST /threads/{threadId}/checkpoints`. langgraph has no HTTP surface in this repository; the string occurs there inside unrelated file paths. |
| `thread id is ` | `CheckpointEndpoint.java` | Coincidence. This is the port's own boundary refusal, `"thread id is N characters; the limit is 239"` — a limit langgraph does not have and a message it does not carry. The fragment happens to appear in langgraph's prose. |
| `window for ` | `BenchRunner.java` | Coincidence. The port's benchmark refusing a window that measured nothing. Unrelated to anything langgraph does. |

Two of the seven — `__error__` is under ten characters and so not reported, but belongs with
the other control channels — are the same category as the four listed: protocol vocabulary,
copied on purpose so the two systems agree about what a write means.

## What licence does that force on this project?

No file carries langgraph's licence into this project, because no file was copied. Behaviour is
not copyrightable, and the four protocol identifiers are too short to be. The port is
distributed under MIT in any case, which is the same licence, so nothing here creates an
obligation the project does not already meet.

## Is behaviour derived even where no text was copied?

**Yes, and that is the point of the project.** Every rule the port implements — the write index
rule and its four exceptions, the strictness of `before`, the conjunctive metadata filter, what
resume restores and what it does not, how a channel value is stored once per version — was
determined by running `langgraph 1.2.11` and `langgraph-checkpoint 4.2.0` and recording what
they did. The port is a reimplementation of langgraph's checkpointing semantics on Akka, and it
is derived work in every sense that matters except the textual one.

Two behaviours are deliberately **not** langgraph's, and the README lists them as decisions:
retention (the port prunes; langgraph keeps everything) and the channel version's suffix
format.

## Also used

- **Akka** — Akka Java SDK 3.6.3, Akka Runtime 1.6.15. Business Source License 1.1.
- **Jackson** — JSON handling in the benchmark runner. Apache-2.0, via the Akka SDK.
- **JUnit 5 and AssertJ** — tests. EPL-2.0 and Apache-2.0 respectively.
