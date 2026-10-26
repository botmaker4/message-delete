# Performance audit baseline

Confirmed from the pre-overhaul implementation:

| Area | Baseline behavior | Consequence |
|---|---|---|
| Successful deletion | `fetch_message` GET then `message.delete` DELETE | 2 API requests per success |
| Application pacing | Fixed 1.0 second before every item | At least 48.6 hours for 174,812 items at one worker, before network time |
| Persistence | `commit()` after every result | Up to one durable transaction per item |
| Dashboard | `db.stats()` and channel query from every item update | Repeated aggregate SQL and render work on the deletion path |
| Work queue | All pending rows loaded into a list and individually queued | Memory proportional to pending message count |
| Inaccessible inference | One fetch-stage 403 bulk-marked the channel | Ambiguous failures could classify many unattempted rows permanently |

The installed `discord.py-self 2.1.0` exposes the supported partial-object path
`Client.get_partial_messageable(id).get_partial_message(id).delete()`. Its delete
implementation calls the library HTTP client's `delete_message` method, retaining
the library's route and global rate-limit handling while avoiding the GET.

Suspected and now instrumented rather than assumed: Discord network latency,
library-internal bucket wait time (not directly exposed), gateway readiness,
server errors, and the distribution of genuine inaccessible channels.
