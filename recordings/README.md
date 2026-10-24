# Recordings

Recorded demo runs for the replay fallback (docs/design.md §10.3, docs/runbook.md §7). Each is one gzipped JSON file written by `director record` (`make record`, `make record-aws`, or at the end of every `make rehearsal`): the chatops-bot's transcript, the rows behind Grafana and the tickets UI, and the starring jobs' metrics.

`make replay` plays the newest one; `make replay RECORDING=recordings/<file> FROM=12m` picks one and a starting point.
