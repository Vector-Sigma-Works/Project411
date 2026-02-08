# Project 411 AI Refresh Schedule

Installed systemd user units:
- project411-ai-refresh.service
- project411-ai-refresh.timer

The timer is set in UTC for this week (EST offset):
- 12:00 UTC (07:00 America/New_York)
- 22:00 UTC (17:00 America/New_York)

Each run:
- runs `node scripts/run_ai_briefing.js`
- restarts `project411-ai-mvs.service`
- appends JSON counts to `runs/ai/refresh.log`

Verify:
```bash
systemctl --user list-timers project411-ai-refresh.timer
journalctl --user -u project411-ai-refresh.service -n 50 --no-pager
```
