# What each report section needs, and where the material is

Required sections come from the course brief (section 4.1), the grading rubric, and the headings
the sample report uses. For each: what it must cover, the material in this folder, and what is
still missing. Page budget: 4-5 pages, ACM `sigconf`.

| Section | Must cover (brief / rubric) | Material here | Diagram or figure | Still open |
|---|---|---|---|---|
| Abstract | problem, approach, headline results | `facts/results.md` sections 1, 2, 6 | | your sentence on the headline result |
| Introduction | background, the two queries + visualization, dataset cited, roadmap | `facts/dataset.md` (size), course brief | | roadmap sentence (sample report has one) |
| Dataset analysis | "strong focus on dataset analysis", long tail, event types, data quirks | `facts/dataset.md` | `longtail-week.png` (in the draft) | your paragraph |
| System architecture | design choices **and motivations**; real-time, complex event processing, scalability; metadata and normalization | `design-decisions.md` D1-D16, evolution table | `01-architecture`, `02-flink-job-graph` | the "alternatives we rejected" paragraph |
| Implementation | hardware / cloud, programming tools, GUI tools; event handling, windowing and EMA state, crossover detection, normalization, metadata usage | `facts/setup.md`, `design-decisions.md` D7-D13, D17-D18, D22-D23 | `03-event-classification`, `02-flink-job-graph` | mostly written in the draft |
| Performance evaluation: setup | hardware and experimental setup (topic page asks for it explicitly) | `facts/setup.md` | `05-experiment-procedure` | **P vs E core split of the M5** |
| Correctness | query results accurate, different event types | `facts/results.md` 1 | | |
| Throughput, scalability | varying CPU, exchanges, event volumes; explain lack of speedup | `facts/results.md` 2-4 | `scalability-throughput`, `subtask-load` (new) | your analysis paragraph |
| Latency under load | latency at several input rates | `facts/results.md` 6 | `04-window-latency-timeline`, latency figures | your analysis paragraph |
| Resource utilization | CPU (and GPU) usage; the sample report has a per-container CPU / memory table | `facts/results.md` 5, `facts/derived-facts.md` | `container-cpu` (new) | decide table or figure |
| Long-tail handling | per-instance load, hot symbols | `facts/results.md` 4, `design-decisions.md` D3, D11 | `subtask-load`, `scalability-skew` | your paragraph |
| Design trade-off (creative exploration) | trade-offs backed by measurements | `design-decisions.md` D5, D19, `facts/results.md` 6-7 | OOO table / figures | your paragraph and why 5 s |
| Visualization | smart visualization (bonus) | `design-decisions.md` D23 | `dashboard-top.png` | optional "why" sentences |
| Conclusion | summary, limitations, future work (sample report has a Future Work subsection) | `facts/results.md` caveats; limitation candidates below | | yours |
| Contributions | each member answers 3 questions; teamwork paragraph | evolution table in `design-decisions.md` (who did what, from git) | | Manas's answers |
| Use of AI | brief disclosure (topic page) | `report/AI_USE.md`, PR list | | your paragraph |
| References | ACM format, consistent | `report/references.bib` | | check the challenge-paper DOI; CCS concepts block |

## Limitation candidates (facts you can draw on)

- One laptop: Kafka, JobManager, TaskManager share one 10-CPU Docker VM.
- One repeat per point on 24 Nov; 3 repeats only for 1/2/4 cores on 17 Nov (spread up to 9% at 1 core).
- Single Kafka broker, replication 1; 8 input partitions.
- Checkpointing off; at-least-once sinks; exactly-once not measured.
- Latency measured to `emitTs` inside Flink, not to a consumer; host and VM clocks differ.
- The real data has no late trades, so the out-of-order trade-off is shown on synthetic data.
- P vs E cores not recorded yet.
- Passively cooled laptop.

## Who did what (from git history, for the contributions section)

| Person | Commits (main) | Areas in the history |
|---|---|---|
| Yashasvi Mishra | 33 (two author spellings) | Kafka/Flink setup, producer to `trading-events`, first Flink job, windowing, EMA + signals, first dashboard, Docker setup, `DECISION.MD` |
| Manas Vernekar (`The5pecial0ne`) | 18 | data exploration, answer key v1-v3 and week answer key, long-tail plot, sample CSVs, ran all Mac experiments, merged the rework PRs |
| Claude Code | 23 | the November rework PRs #2-#4 and results PR #6 (disclose in the AI section) |

Check this against what you remember; git only shows who committed, not who designed.
