#!/usr/bin/env bash
# Fail unless the Caddy on PATH is at least the security floor (default 2.11.4).
#
# Why: the 2026 Caddy advisory cluster (handler/placeholder re-expansion
# CVE-2026-77281, unbounded body DoS CVE-2026-92284, admin auth bypass
# CVE-2026-92700, FastCGI header bypass CVE-2026-52845) is fixed only in 2.11.4;
# earlier 2026 issues are fixed in 2.11.3. Hosts that install Caddy with a bare
# `apt-get install -y caddy` take whatever the feed serves, so the running
# version must be asserted at deploy time, before the Caddyfile is reloaded.
#
# `caddy version` prints the version first, then the build integrity hash:
#     v2.11.4 h1:XKxkMTgNSizEvKG6QHue6cAsFOteU2qA61w2tKkCWi0=
# Development builds print `(devel)` plus a pseudo-version (e.g. 2.11.4-0.2026...).
#
# Usage: REQUIRED_CADDY_VERSION=2.11.4 verify-caddy-version.sh
set -euo pipefail

REQUIRED="${REQUIRED_CADDY_VERSION:-2.11.4}"

raw="$(caddy version 2>/dev/null || true)"
if [ -z "$raw" ]; then
    echo "ERROR: 'caddy version' produced no output; is Caddy installed?" >&2
    exit 1
fi

# "v2.11.4 h1:..." -> "2.11.4"; "(devel) h1:..." -> "(devel)".
got="$(printf '%s\n' "$raw" | awk '{print $1}' | sed 's/^v//')"

case "$got" in
    [0-9]*) ;;
    *)
        echo "ERROR: cannot parse a release version out of 'caddy version' -> '${raw}'" >&2
        echo "       required >= ${REQUIRED}; rebuild with a released version." >&2
        exit 1
        ;;
esac

# sort -V orders the pair; the lower element is the effective version.
lowest="$(printf '%s\n%s\n' "$REQUIRED" "$got" | sort -V | head -n1)"
if [ "$lowest" != "$REQUIRED" ]; then
    echo "ERROR: Caddy ${got} is older than the required floor ${REQUIRED}." >&2
    echo "       Advisory cluster (CVE-2026-77281, CVE-2026-92284, CVE-2026-92700," >&2
    echo "       CVE-2026-52845) is fixed only in 2.11.4 and later." >&2
    echo "       Full output: ${raw}" >&2
    exit 1
fi

echo "OK: Caddy ${got} >= required ${REQUIRED} (${raw})"
