# Lambda Pipeline

Schedule jobs (lambda functions) based on:
- Schedule (cron)
- The output of other jobs

This service is responsible for
- Running the jobs at the appropriate time
- Recording the output
- Retrying failed jobs

## Parts

API
- Responsible for powering frontend

Cron Manager
- Creates runs based on cron schedule of jobs

Run Launcher
- Performs runs, records results
- Launches additional runs based on failure/retry logic and if jobs are dependent on running job
- Maybe keep abstract for non-lambda?

## Cron edge case scenarios
- Add job, add to cron manager. Next run is at next cron occurence
- Job added, server shutdown resulting in missed crons. When server starts, pick up next cron run. Have option for backfilling missed runs
- Job removed, remove from cron manager
- Cron scheduled and then changed, delete old cron, add new
