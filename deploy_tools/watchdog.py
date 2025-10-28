#!/usr/bin/env python3
import re, subprocess, time, os, sys

LOGFILE = "/tmp/sniper_live.out"   # where start.sh logs to (see go_live.sh)
BACKUP = "/opt/render/project/src/.env.bak.last"  # set as created by your go_live flow
ROLLBACK_SCRIPT = "/opt/render/project/src/deploy_tools/rollback.sh"
PATTERNS = [
    (re.compile(r'JUP_BREAKER_RESET'), 3, 60),   # 3 occurrences within 60s -> rollback
    (re.compile(r'ROUTEGUARD_RESET|ROUTEGUARD_MAX_IMPACT_BPS'), 2, 40),
    (re.compile(r'quote.*error|QuoteError|no route'), 5, 120),
]

def tail(file):
    p = subprocess.Popen(['tail','-n','0','-F', file], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p

def main():
    if not os.path.exists(LOGFILE):
        print("Logfile not found:", LOGFILE); sys.exit(1)
    p = tail(LOGFILE)
    window = []
    while True:
        line = p.stdout.readline()
        if not line:
            time.sleep(0.1); continue
        now = time.time()
        window.append((now, line))
        # expire
        window = [(t,l) for (t,l) in window if now - t < 180]
        for pattern, count, secs in PATTERNS:
            matches = [1 for (t,l) in window if pattern.search(l) and now - t < secs]
            if len(matches) >= count:
                print("ALARM: pattern", pattern.pattern, "seen", len(matches), "times; triggering rollback")
                subprocess.call([ROLLBACK_SCRIPT, BACKUP])
                sys.exit(0)

if __name__ == "__main__":
    main()
