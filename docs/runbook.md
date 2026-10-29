# Demo-day runbook

How to run the three demos live at Current 2026, and what to do when something goes wrong. The design is in [design.md](design.md); this is the operating procedure.

The short version:

- **The live demo runs on EC2.** The laptop only shows the UIs, through an SSM tunnel. The laptop's own stack is the fallback.
- **Everything is scripted.** One command starts a 30-minute timeline of deploys and faults; the agents do the rest. You do two things by hand: ask `@ops-assistant` a question and click **Approve** (Scenario 2), and optionally click F4's **Restart with state on disk** (Scenario 3).
- **If the live demo fails, replay a recording.** `make replay` plays a recorded rehearsal back into the same Mattermost, Grafana and tickets UI, with no agents, models or network. It looks the same, so say that you're switching to a recording.

Commands below run from the repo root on the laptop. `make …-aws` targets act on the EC2 instance (`AWS_PROFILE=evidentsystems`).

## Contents

1. [The fallback ladder](#1-the-fallback-ladder)
2. [The day before](#2-the-day-before)
3. [The morning of the talk](#3-the-morning-of-the-talk)
4. [Screen setup](#4-screen-setup)
5. [Showtime: the happy path](#5-showtime-the-happy-path)
6. [When something goes wrong](#6-when-something-goes-wrong)
7. [Replaying a recording](#7-replaying-a-recording)
8. [After the talk](#8-after-the-talk)
9. [Reference](#9-reference)

## 1. The fallback ladder

Go down the ladder only as far as you have to. Each rung is slower to recover from than the one above it, but depends on less.

| Rung | When | What you do | Needs |
|---|---|---|---|
| 0. Keep talking | Something is slow but still working: Claude takes two minutes, a signal is a minute late | Fill with the talking points in §5; the timeline has slack | Nothing |
| 1. Fix in place | A browser tab, the tunnel or one step misbehaves | The specific fix in §6 (reload, reconnect, a manual rollback) | EC2 and the network |
| 2. Replay on EC2 | The live run went wrong (agents, models or a job), but EC2 and the network are fine | `make replay-aws FROM=<where you are>` | EC2 and the network |
| 3. Replay on the laptop | The network or EC2 is gone | Stop the tunnel; `docker compose start`; `make replay FROM=<where you are>` | The laptop's stack, built and stopped |
| 4. Video | The laptop's stack is broken too | Play the screen recording of a rehearsal | A video player |

Rungs 2 and 3 replay the same recording into the same UIs, so the audience sees the same thing. Rung 3 needs no network at all: no AWS, Jev or Anthropic.

## 2. The day before

Allow half a day. Do it at the venue's hotel if you can, on the network you'll have.

### 2.1 Bring up EC2

```sh
make stack-aws          # the CloudFormation stack (VPC, instance, bucket, Glue, ECR)
make secrets-aws        # ShadowTraffic license and the API keys from .env → SSM
make deploy-aws         # build and push the images, start the stack on the instance (~15 min)
make tunnel             # in its own terminal tab; leave it running
make preflight-aws      # go/no-go on the instance, then the tunnel
```

`make preflight-aws` should end with **Go.** A warning that no reset has happened yet is expected at this point.

### 2.2 Rehearse twice on EC2

```sh
make rehearsal-aws                                            # starts it on the instance, returns
scripts/aws.sh exec tail -30 .director/rehearsal.log          # follow it; ~45 min
```

A rehearsal resets the platform, runs the whole timeline with Scenario 2's engineer played by script, checks every scenario, writes the timings and peak memory, and records the run. It must end with **Rehearsal passed without intervention.** Do two in a row. If one fails, read its log (`.director/rehearsals/<start>/` on the instance) and §6 before trying again.

Watch at least one of them through the tunnel, in the screen setup of §4, to rehearse the narration against the real timing.

### 2.3 Keep the fallbacks ready

1. **Fetch the good recording** from the instance. Rehearsals record themselves; this copies the newest to `recordings/` on the laptop:

   ```sh
   scripts/aws.sh exec ls -t recordings          # pick the latest rehearsal's recording
   scripts/aws.sh fetch recordings/<name>.json.gz
   ```

   Commit it, so every clone has it. The deploy bundle includes `recordings/`, so it's on EC2 for rung 2 as well.
2. **Bring up the laptop's stack** and leave it up: `make up`, then `make preflight`. Do it on the hotel network: `make up` builds images and pulls the base images, which needs the internet. Then replay the recording on it once (`make replay`, watch a few minutes, `make reset`) so you know rung 3 works on this laptop. The laptop's stack and the tunnel use the same ports, so **stop the laptop's stack with `docker compose stop` while you use the tunnel**. `docker compose start` brings it back in about a minute and needs no network, because it neither builds nor pulls (`make up` builds, and may need the network).
3. **Screen-record one rehearsal** (QuickTime, the full screen layout of §4), for rung 4. Keep it on the laptop's disk, not in the cloud.

### 2.4 Overnight

Leave the instance running; it costs well under a dollar an hour, and a running stack is one less thing to start in the morning. If you'd rather stop it: `scripts/aws.sh stop`, and in the morning `scripts/aws.sh start`, which also starts the stack. Allow 10 minutes for that.

## 3. The morning of the talk

| When | What | Command |
|---|---|---|
| T−3 h | Laptop on power; notifications off (Focus: Do Not Disturb); close Slack, mail, calendars | |
| T−3 h | Tunnel up, in its own terminal tab | `make tunnel` |
| T−3 h | Go/no-go | `make preflight-aws` |
| T−2.5 h | A warm-up rehearsal, if there's time: it warms every JVM and checks the day's Jev and Anthropic latency | `make rehearsal-aws` |
| T−30 min | Check the venue's network from the stage, with the projector connected | `scripts/preflight.sh --tunnel` |
| T−15 min | **Final reset**; it takes about 2 minutes | `make reset-aws` |
| T−10 min | Go/no-go again: it should say *reset … ready for 'make run'* | `make preflight-aws` |
| T−10 min | Set up the screens (§4) and log in to Mattermost as `oncall` | |

Don't start the run until you're on stage and ready for it (§5.1). After the reset, the platform idles quietly with healthy jobs; nothing happens until `make run-aws`.

If the venue network fails the T−30 check, switch to your phone's hotspot and check again. If neither works, plan on rung 3: stop the tunnel, `docker compose start` on the laptop, then `make replay` instead of the live run.

## 4. Screen setup

Use one browser window, full screen, with these tabs in order. The tunnel puts every UI on the same `localhost` port as locally, so the same bookmarks work for live, replay and the laptop's stack.

1. **Mattermost** `http://localhost:8065/platform/channels/platform-alerts`, logged in as `oncall` (password in `.env` as `MATTERMOST_ADMIN_PASSWORD`). Favorite the three channels so they sit together in the sidebar: `platform-alerts`, `platform-signals`, `platform-incidents`. Decide whether threads open in the side pane or inline (Settings → Display → Collapsed Reply Threads), and rehearse with that setting.
2. **Grafana** `http://localhost:3000/d/platform-overview`: alerts in vs. signals out, and the four starring jobs. Its time range is the last hour, refreshing every 10 s; leave it.
3. **Tickets** `http://localhost:8094`.
4. **Flink** `http://localhost:8081`, if you want to show real jobs running.

Keep one terminal tab for commands (`make status-aws`) where you can reach it without the audience seeing it, and the tunnel's tab in the background. Zoom the browser to 125–150% so the back of the room can read the cards.

## 5. Showtime: the happy path

### 5.1 Starting the run

```sh
make run-aws        # starts the 30-minute timeline on the instance and returns
make status-aws     # where the run is, and the incidents so far
```

The timeline's clock starts when you run it. The fleet's noise builds from the first minute, and the first genuine signal arrives within 4 minutes. Start it as you begin the part of the talk before Scenario 1, so there is noise to show by the time you get there. The clock below says what happens when; keep `make status-aws` handy to see where you are.

### 5.2 The run clock

Times are from the start of the run, measured on EC2 rehearsals (§9.3 has the full numbers). Allow a minute either way.

| Clock | What happens | Where to look |
|---|---|---|
| 0:00 | `trade-events-ingest` 2.18.0 deployed: F2's red herring | |
| 1:00 | `positions-aggregator` 0.9.0 deployed: F4's state starts growing | |
| 1–4 | The fleet's alerts start scrolling | `#platform-alerts`, Grafana's *Alerts in* |
| 0–4 | First signal: a fleet job's crash loop | `#platform-signals` |
| 11:00 | F2: `oms-apac-1` starts sending malformed trades | |
| ~12 | **Scenario 2 cue:** `trade-events-ingest` signal | `#platform-signals` |
| ~14–16 | `positions-aggregator` signal (state growth) → incident, escalated with buttons | `#platform-incidents` |
| 15:00 | F1: `payments-enrichment` 1.42.0 deployed | |
| ~16 | `payments-enrichment` signal → incident → **rolled back** by the operator within 2 minutes | `#platform-incidents` |
| ~18 | F1 verified and its ticket resolved | `#platform-incidents`, tickets |
| 18:00 | F3: `market-data` at 9× for 10 minutes | Grafana: *Lag* for `market-data-to-iceberg` |
| ~22–24 | `market-data-to-iceberg` signal → **rescaled** 2 → 4 | `#platform-incidents` |
| ~25–27 | Lag drains; rescale verified, ticket resolved | Grafana *Lag*, `#platform-incidents` |
| 30:00 | End of the timeline. Nothing stops: the cards, threads and dashboards stay | |

### 5.3 Scenario 1: amplify signal, suppress noise

Show `#platform-alerts` scrolling, then `#platform-signals`, nearly empty by comparison. Grafana's top row counts alerts in, signals raised, alerts attached to incidents and alerts suppressed.

Open a signal card and point out:

- the failure class, impact and the probabilities: Jev's judgments;
- that the card's text and the decision to raise it are code, from those judgments (`triage.yaml`'s thresholds).

Talking points while you wait for the next signal: the naive alert rules (static thresholds) are deliberate; about 1,000 simulated jobs report the same metrics as the real ones; the known-noise notes.

### 5.4 Scenario 2: human-initiated, agent-assisted (F2)

1. When the `trade-events-ingest` card appears in `#platform-signals` (about 12 minutes in), open its thread.
2. Reply: **`@ops-assistant what's going on here? Was it the deploy?`**
3. Claude's tool calls stream into the thread as status lines (*🔎 checking deployment history…*, *🧪 sampling `trade-events`…*). The answer took 15–21 seconds in rehearsals; allow up to 3 minutes on a slow day. It should say the deploy is a red herring and name `oms-apac-1`'s `B`/`S` records.
4. An approval card follows: **redeploy 2.18.0 with `--on-deser-error=dlq`**, plus a ticket for the `oms-apac` team. Click **Approve**.
5. The bot redeploys through the pipeline and posts the new ticket's card. Optionally ask **`@ops-assistant is it healthy now?`**

While Claude works: it only has read-only tools and one that *proposes*; nothing changes until a person clicks Approve, and every decision is audited in `platform.actions`.

F1 deploys at 15:00, so the operator may roll it back while you're still in this thread. That's the hand-off to Scenario 3: "while we were talking, the platform fixed something on its own."

### 5.5 Scenario 3: agent-initiated (F1, F3, F4)

Go to `#platform-incidents`. Each incident is a card with its thread of work notes, and a ticket.

- **F1** `payments-enrichment`: signal → incident → rollback 1.42.0 → 1.41.0 → verified → ticket resolved, all within about 2 minutes. Open the thread: each step's note, and the guardrail checks that allowed the rollback.
- **F4** `positions-aggregator`: escalated, not fixed. The card explains why (state growth needs a code fix) and offers **Acknowledge** and **Restart with state on disk (temporary)**. Clicking the restart is the "human only when necessary" moment; the operator carries it out.
- **F3** `market-data-to-iceberg`: rescaled from 2 to 4. Switch to Grafana's *Lag* panel and watch it drain over a few minutes.

## 6. When something goes wrong

Find the symptom, then do the first thing that applies. "Replay" means §7: rung 2 if EC2 and the network are fine, rung 3 if not.

### 6.1 Network and access

| Symptom | Likely cause | Do this |
|---|---|---|
| A UI tab stops loading, or Mattermost shows *Unable to connect* | The tunnel dropped for that port | Wait 5 s: `make tunnel` reconnects by itself (its tab says *dropped; reconnecting*). Then reload the tab |
| The tunnel's tab keeps saying *dropped* | The laptop lost the internet | Switch to the phone's hotspot. If there's no internet at all: rung 3 |
| `make …-aws` hangs or fails with an SSM error | AWS unreachable, or expired credentials | `aws sts get-caller-identity --profile evidentsystems`; re-authenticate. Meanwhile the run on EC2 carries on without you |
| Everything is unreachable and there's no hotspot | Venue network | Rung 3: stop the tunnel (Ctrl-C), `docker compose start` (about a minute), then `make replay FROM=<clock>` |

### 6.2 The models

| Symptom | Likely cause | Do this |
|---|---|---|
| Alerts keep scrolling but no signals appear for several minutes | Jev slow or down | `scripts/aws.sh exec curl -s localhost:8093/stats`: `judgments` should rise, and a rising `errors` means Jev is failing. If Jev is down, the agents retry, and the signals arrive late once it's back. If it stays down: replay from the current clock |
| Claude's status lines stop for over 2 minutes | Anthropic slow | Keep talking; the timeout is 5 minutes. If it fails, it says so in the thread (⚠️); ask again in the same thread |
| Claude answers but blames the deploy, or proposes something else | The model's judgment | Click **Reject** (that's the human in the loop working), then ask: *"@ops-assistant could it be the input data? Sample the records it failed on."* |
| Claude's answer is fine but there's no approval card | It didn't call `propose_remediation` | Ask: *"@ops-assistant propose the fix."* |
| `@ops-assistant` replies that it isn't set up | No `ANTHROPIC_API_KEY` on the instance | Not fixable on stage: replay Scenario 2 (`make replay-aws FROM=12m`), or narrate over the recording |

### 6.3 The platform

| Symptom | Likely cause | Do this |
|---|---|---|
| No `trade-events-ingest` signal by about 15:00 | F2 not injected, or triage slow | `make status-aws`: the 11:00 step should be ✓. If it failed, `scripts/aws.sh director inject f2`. Otherwise give it two more minutes, then replay from 12m |
| F1 is escalated instead of rolled back | A guardrail stopped it, or Jev picked differently | Narrate it: the agent declines when it isn't sure. Open the thread to show which check failed. To show the fix anyway: `scripts/aws.sh exec scripts/cicd.sh rollback payments-enrichment` |
| No `market-data-to-iceberg` signal by about 25:00 | F3's backlog built slowly, so triage's first judgment fell just short and it's waiting for a recheck (2, then 4 minutes) | Talk through F4 meanwhile, and show the lag climbing in Grafana. If it hasn't come by 27:00, replay from 21m for the rescale |
| The operator rescaled F3 but lag isn't falling | The rescale didn't take | `scripts/aws.sh exec curl -s localhost:8081/jobs/overview`; narrate the verification step: if lag doesn't drain within 6 minutes, the operator escalates by itself |
| A card's button does nothing | The bot didn't get the click | Click again once; Mattermost shows *Someone already decided* if the first one counted |
| Grafana's panels are empty | Time range or the `job` variable changed | Reload the dashboard (it resets to the last hour and all four jobs) |
| A Flink job is stuck in RESTARTING outside the timeline, or a TaskManager died | The platform itself | Don't fix it live: replay from the current clock |
| `make preflight-aws` says the Docker disk is over 90% | Iceberg or Docker leftovers | Before the talk only: `scripts/aws.sh exec docker system prune -f`, and preflight again |

### 6.4 Time

| Situation | Do this |
|---|---|
| You're running late | Nothing breaks. The cards, threads, tickets and Grafana stay as they were; scroll back. Ask Scenario 2's question whenever you get there |
| You're early: the next cue is minutes away | Talk through the current cards, open a thread, show the ticket or the guardrails in `operator.yaml`. `make status-aws` shows the next step |
| You need to start again | A reset takes about 2 minutes and the noise needs a few more to build up. If you're short of time, replay instead |

## 7. Replaying a recording

A recording is a rehearsal's chat transcript, the rows behind Grafana and the tickets UI, and the four starring jobs' metrics (design.md §10.3). Replaying one plays it back, from any moment, into the same UIs, without the agents, Jev, Claude or ShadowTraffic.

### 7.1 Starting

On the laptop (rung 3), with the tunnel stopped and the laptop's stack started (`docker compose start`, about a minute; `make preflight` shows when it's ready, though it will report the models unreachable when offline):

```sh
make replay                      # the newest recording in recordings/, from the start
make replay FROM=12m             # from 12:00: everything before appears at once
make replay RECORDING=recordings/<name>.json.gz FROM=15m SPEED=2
```

On EC2 (rung 2), through the tunnel; it plays the newest recording on the instance, which is the morning's warm-up rehearsal if you ran one:

```sh
make replay-aws FROM=12m
```

`replay start` first quiets the platform: it stops the fault injectors, the generators, the agents and Alertmanager, cancels the Flink jobs, empties the history and tickets, and archives the channels. That takes under 30 seconds. Then the recording plays.

- **Choosing `FROM`:** use the run clock where the live demo went wrong, a minute or so earlier. Everything before it appears at once (the alert firehose only for its last two minutes), so the threads and counters look as they did.
- **Reload the browser tabs** after it starts: the channels are new. Starting late in the recording, allow up to a minute for the earlier threads to fill in.

### 7.2 Steering it

```sh
scripts/director.sh replay status          # where it is, and the next timeline step
scripts/director.sh replay pause           # hold here, e.g. while you explain a card
scripts/director.sh replay resume
scripts/director.sh replay seek 18m        # jump forward (never back)
scripts/director.sh replay stop
```

On EC2, prefix them with `scripts/aws.sh` (`scripts/aws.sh director replay pause`); each takes a few seconds through SSM.

### 7.3 What's different from live

- **The human steps are part of the recording.** The question to `@ops-assistant` appears as `oncall` at the recorded moment, and the cards update when the recorded clicks happened. Clicking a replayed button does nothing; narrate instead, or click it as the recorded update lands.
- **Grafana's per-job panels start from when the replay started**; the counters include everything before it.
- **The tickets UI shows each ticket in its final state** from the moment it was opened. Its cards in Mattermost update step by step, as recorded.
- **The Flink UI shows no jobs**: the replay cancels them.
- **Times quoted in messages are the recording's.** Claude's answer says, for example, *deployed at 00:23*, and Mattermost stamps the posts with now. Few will notice; don't point at the clock.

Tell the audience you're switching to a recording of a rehearsal on the same platform. It's the same demo, and they'll trust the rest more for it.

### 7.4 Going live again

`make reset` (or `make reset-aws`) stops the replay and resets the live platform, in about 2 minutes.

## 8. After the talk

1. If anything went wrong, save the evidence before resetting: `make record-aws` and `scripts/aws.sh exec tar czf /tmp/director.tgz .director`.
2. **Tear down EC2** when you no longer need it: `make teardown-aws` deletes the stack, the bucket's contents, the images and the secrets. It can't be undone.
3. Laptop: `make down` (or `make clean` to delete the data too).

## 9. Reference

### 9.1 Commands

| What | Laptop | EC2 |
|---|---|---|
| Go/no-go | `make preflight` | `make preflight-aws` |
| Reset (~2 min) | `make reset` | `make reset-aws` |
| Start the run (30 min) | `caffeinate -is make run` | `make run-aws` |
| Where are we? | `make status` | `make status-aws` |
| Full rehearsal (~45 min) | `caffeinate -is make rehearsal` | `make rehearsal-aws` |
| Record the latest run | `make record` | `make record-aws` |
| Replay | `make replay FROM=…` | `make replay-aws FROM=…` |
| Steer a replay | `scripts/director.sh replay pause\|resume\|seek <t>\|status\|stop` | the same, after `scripts/aws.sh` |
| Inject a fault by hand | `scripts/director.sh inject f2` | `scripts/aws.sh director inject f2` |
| Roll a job back by hand | `scripts/cicd.sh rollback <job>` | `scripts/aws.sh exec scripts/cicd.sh rollback <job>` |
| Container status | `make ps` | `make ps-aws` |

On the laptop the director runs from cargo and waits on the wall clock: if the laptop sleeps, overdue steps run on waking. Prevent sleep with `caffeinate -is`.

### 9.2 Accounts and URLs

| | URL | Login |
|---|---|---|
| Mattermost | `http://localhost:8065` | `oncall` / `MATTERMOST_ADMIN_PASSWORD` (default `oncall-demo-2026`) |
| Grafana | `http://localhost:3000/d/platform-overview` | none needed to view |
| Tickets | `http://localhost:8094` | none |
| Flink | `http://localhost:8081` | none |
| Prometheus | `http://localhost:9090` | none |

### 9.3 Rehearsal timings and memory

Measured by `make rehearsal` and `make rehearsal-aws`. Times are from the start of the run; each rehearsal's `timings.txt` has the full list.

| What the audience sees | Laptop, 200 jobs | EC2 #1, 1,000 jobs | EC2 #2, 1,000 jobs |
|---|---|---|---|
| First signal (a fleet job's crash loop) | 3:10 | 3:43 | 0:16 |
| F2 `trade-events-ingest` signal | 12:05 | 12:03 | 12:06 |
| Claude's answer, after the question | 21 s | 15 s | 20 s |
| F2 redeployed with the DLQ (scripted click) | 12:33 | 12:23 | 12:29 |
| F4 `positions-aggregator` signal, escalated | 15:50 | 16:19 | 14:22 |
| F1 `payments-enrichment` signal | 16:05 | 16:18 | 16:06 |
| F1 rolled back | 16:07 | 16:19 | 16:08 |
| F1 verified, ticket resolved | 17:38 | 17:49 | 17:38 |
| F3 `market-data-to-iceberg` signal | 26:21 \* | 23:49 | 21:52 |
| F3 rescaled 2 → 4 | 26:26 \* | 24:01 | 22:04 |
| F3 verified, ticket resolved | 29:26 \* | 27:01 | 25:05 |
| F3 lag at the rescale → at verification | 59,188 → 13,448 \* | 22,702 → 0 | 10,661 → 0 |
| Alerts in; other jobs raised (noise) | 351; 0 | 1,660; 3 | 1,659; 2 |
| Rehearsal passed without intervention | yes | yes | yes |

\* Before the triage recheck went from 5 minutes to 2 (design.md §8.1). Replaying the same run with 2-minute rechecks raised F3 three minutes sooner, as the EC2 rehearsals did.

The fleet's first signal varies: the fleet starts at the reset, so a crash loop can already be failing when the run starts. Scenario 2's question and click were scripted; a person typing takes a minute longer, which the timeline absorbs.

Peak memory per container, from the same rehearsals (design.md §11 compares them with the budget):

| Container | Laptop | EC2 #2 |
|---|---|---|
| TaskManagers | 2,501 MiB (one) | 3,127 + 1,018 MiB (two) |
| JobManager | 934 MiB | 1,369 MiB |
| Kafka | 476 MiB | 1,229 MiB |
| RustFS (laptop only) | 1,024 MiB, at its limit | — |
| ShadowTraffic: fleet, inputs, F3, F2 | 420 + 386 + 319 + 289 MiB | 673 + 390 + 325 + 282 MiB |
| Prometheus | 222 MiB | 708 MiB |
| SQL Gateway, Iceberg REST (laptop only) | 385 + 344 MiB | 435 MiB |
| Grafana, Postgres, Mattermost | 213 + 128 + 136 MiB | 281 + 223 + 155 MiB |
| chatops-bot (with Claude Code) | 135 MiB | 129 MiB |
| Rust services and agents | under 20 MiB each | under 50 MiB each |
| **Peak of the total** | **6.8 GB of Docker's 9 GB** | **9.8 GB of 31 GB** |

Samples are 30 s apart, so a short Claude Code run's peak may fall between them.
