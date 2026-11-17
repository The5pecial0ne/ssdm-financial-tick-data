# AI-use disclosure (template)

The course allows coding agents and other development tools, and AI for language proofreading
of the report. It requires a short disclosure of how AI tools were used. Fill this in honestly
and in your own words, then condense it into the "Use of AI Tools" section of the report.

## Tools used
- Which tools (for example Claude / Claude Code), and roughly when.

## In the system (code)
Facts you can draw on. Check them against your own memory of the project, and keep or drop as true:
- Code written by the team without AI: ...
- An AI coding agent (Claude Code) was used in November 2026 to: rewrite the Flink job into
  the `ssdm` package (event classification, per-partition watermarks, window-close tie-break,
  subscriptions, side output for late trades); rewrite the producer; add the reference,
  validator and synthetic-data tools; add the Docker images and experiment scripts; rebuild
  the dashboard. These changes came in through pull requests #2, #3 and #4.
- How you verified it: you reviewed the pull requests, re-ran the validation and the
  experiments on your own machine, and ... (describe what you checked and understood).

## In the report
- AI was used only for language proofreading of text we wrote: yes / no, which sections.
- All analysis, interpretation of results and conclusions are our own.

## Responsibility
We have read and understood all submitted code and results, and take responsibility for them.
