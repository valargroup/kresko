# Transparent transaction workload

`txblast-transparent-local` generates transparent self-spends on a private Kresko chain. It uses the local-genesis upgrade schedule from `KRESKO_LATEST_NETWORK_UPGRADE` and `KRESKO_NU7_ACTIVATION_HEIGHT`. Set these variables to match the daemon.

The generator requires confirmed **non-coinbase** funds. It excludes coinbase outputs even after maturity because consensus requires shielding coinbase funds before transparent spending. The existing `fund-runtime-keys-local` command can shield treasury funds and unshield them into runtime addresses.

```sh
KRESKO_LATEST_NETWORK_UPGRADE=nu7 kresko txblast-transparent-local \
  --rpc-endpoint http://127.0.0.1:18232 \
  --funded-key-path /path/to/funded_key.json \
  --journal /path/to/transparent-journal.jsonl \
  --rate 12 --lanes 2048 --concurrency 16 \
  --null-data-bytes 23 --trace /path/to/transparent-events.jsonl
```

Use one journal for each wallet and chain. Keep the journal when restarting. Do not run another spender against the same wallet. The generator locks the journal and verifies its chain genesis and public wallet address. The journal contains signed transactions, but it contains no spending key.

The generator splits confirmed funds into up to 2,048 outputs per fanout transaction. `--lanes` sets the total target, up to 65,536. It spends confirmed outputs only. It waits for each spent output's replacement to confirm before spending that replacement. `--concurrency` limits signing and RPC tasks together. `--rate` limits new transactions and retries; missed ticks do not trigger catch-up bursts.

The generator syncs each reservation to disk before submission. If RPC fails after accepting a transaction, the generator retries the same signed bytes. It retains confirmed reservations until expiry to protect their inputs through reorgs. It clears expired transactions only after a stable chain/mempool snapshot. If a later deep reorg crosses a discarded transaction's expiry, the generator stops and requires wallet reconciliation. Do not bypass that stop by deleting the journal.

The default 23-byte OP_RETURN payload produces approximately 241–242-byte transactions. Each transaction costs 10,000 zats and has one legacy sigop. An 80-byte payload produces approximately 300-byte transactions at 20,000 zats. Signature length can change serialized size by a byte. The generator calculates fees from the actual output scripts.

For 80 nodes, 12 transactions/second/node supplies approximately 231–232 KB/second before acceptance losses. This provides initial headroom over 4,000,000 bytes/20 seconds and 4 MiB/20 seconds. These byte limits differ. Select the active daemon limit explicitly when configuring the experiment.

If every generated transaction confirms, 12 transactions/second costs each node 4.32 coins/hour in fees. Under that condition, a 10-coin seed balance lasts roughly 2.1–2.2 hours after fanout fees and minimum-output reserves. Rejected or expired transactions pay no fee unless they enter a block. Provide more non-coinbase funds before balances run low. The generator discovers new confirmed outputs during refresh and expands them into usable lanes. Existing low-value outputs do not count toward the target. The generator reports `funding_needed` when confirmed funds cannot pay another transaction fee and minimum output.

`fund-runtime-keys-local` reads `treasury_key.json` and `funded_keys.json` from its `--local-genesis-dir`. For per-node funding, prepare a separate directory with that node's funded-key JSON as `treasury_key.json` and a one-entry `funded_keys.json` array containing the same key. Preserve the original key files. Run the command against that node with an appropriate `--minimum-recipient-zats` and confirmation timeout. Supply mature treasury coinbase outputs when replenishing; the generator does not spend those outputs. Funding creates shielded transactions and requires blocks to confirm both steps.

The console reports progress. `--trace` writes build, acceptance, uncertain submission, snapshot, funding, and stop events as JSONL. Accepted bytes measure successful RPC responses during this process. They do not measure canonical block inclusion. Verify block bytes, mempool depth, and generator health before calling an experiment full-block. `--duration-secs` bounds a local test; zero runs until interrupted. Ctrl-C preserves all submitted reservations for restart.

The daemon also needs enough mempool **transaction cost** capacity. Its default `mempool.tx_cost_limit = 80000000` holds at most 8,000 small transactions because ZIP401 charges at least 10,000 cost units per transaction. That cannot fill a 4 MB block with this profile. A limit of 320,000,000 holds approximately 32,000 such transactions. `mempool.max_transaction_bytes` limits each transaction separately; increasing it does not enlarge aggregate capacity. A 250,000-byte individual limit is sufficient for this workload. Apply a mempool cost change through a planned daemon restart before starting the loaded phase.
