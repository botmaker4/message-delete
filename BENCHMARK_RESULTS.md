# Controlled benchmark summary

Run on 2026-10-26 with `python benchmarks/benchmark_simulated.py`. Fake Discord
objects were used; no live requests were made. Protocol-mandated retry sleeps are
excluded so the results isolate request count, scheduler, and SQLite overhead.

| Scenario | Messages | Optimized requests | Optimized commits | Simulated execution |
|---|---:|---:|---:|---:|
| Accessible success | 10,000 | 10,000 | 406 | 0.719 s |
| One confirmed inaccessible channel | 10,000 | 2 | 7 | 0.046 s |
| 50% accessible / 50% inaccessible | 10,000 | 5,002 | 207 | 0.390 s |
| Already missing | 10,000 | 10,000 | 406 | 0.828 s |
| 429 once, then success | 100 | 200 | 6 | 0.016 s excluding Retry-After |
| 503 once, then success | 100 | 200 | 6 | 0.015 s excluding backoff |

Peak traced memory across the suite was 2,548,154 bytes. The legacy successful
path required 20,000 requests and 10,000 per-result commits for 10,000 messages;
it also imposed 10,000 seconds of configured delay before network time. The new
successful path cuts requests by 50% and measured commits by about 96%. The new
inaccessible-channel result uses two requests because it deliberately confirms
the initial message-level denial with a channel-level probe; the earlier one-request
bulk inference was faster but not sufficiently reliable.

These are deterministic local simulations, not predictions of Discord throughput.
Real completion time remains controlled by Discord's route/global buckets, network
latency, inaccessible distribution, and account/session state.
