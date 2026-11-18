//! Transparent load: fills blocks with ordinary 1-in-1-out P2PKH transactions.
//!
//! Setup shields each source key's mature funds into one Orchard note (transparent
//! coinbase may only be spent to shielded outputs), unshields them into fan UTXOs at
//! the blaster's own address, and splits each fan UTXO into up to [`FAN_OUTPUTS`]
//! lanes. Each lane then spends itself back to the same address, once per block it
//! confirms in, at the requested total rate. Events use the Orchard mode's schema, so
//! the same trace tooling reads both.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use zcash_address::ZcashAddress;
use zcash_transparent::address::TransparentAddress;

use crate::commands::fund_runtime_keys::{select_spendable_treasury_utxos, shield_and_unshield};
use crate::config::LocalGenesisFundedKey;
use crate::txblast::orchard::state::{RegistrySnapshot, TreasurySnapshot};
use crate::txblast::orchard::tracing::EventContext;
use crate::txblast::orchard::{
    OrchardTxblastTracer, PendingTxCounts, RuntimePhase, TransparentInput, build_transparent_tx,
    transparent_fee,
};
use crate::txblast::rpc::ZebraRpcClient;
use crate::txblast::transparent::{FundedKey, funded_key_from_raw};
use crate::txblast::transparent_sized;
use crate::txblast::{TxblastNetworkParams, TxblastTraceConfig, local_target_height};

/// Most lanes one fan-out transaction creates, which keeps each fan-out near 17 KB.
const FAN_OUTPUTS: usize = 500;
/// Least value a new lane is funded with.
const MIN_LANE_VALUE: u64 = 20_000;
/// Times one signed spend is sent when the node's reply is lost or unreadable.
const MAX_SUBMIT_ATTEMPTS: u32 = 3;
/// Wait before resending the same signed bytes.
const RESUBMIT_DELAY: Duration = Duration::from_secs(1);
/// How long setup waits for each funding transaction to confirm.
const SETUP_CONFIRM_TIMEOUT: Duration = Duration::from_secs(600);
/// Mature UTXOs shielded per source key.
const MAX_SOURCE_UTXOS: usize = 64;
/// Blocks a submitted spend may go unmined before the lane checks whether the node
/// still holds it. A full pool evicts accepted spends, and an evicted spend's input
/// is still unspent, so the lane can spend it again.
const STALE_BLOCKS: u32 = 3;

/// Settings for a transparent load run.
pub struct TransparentLoadConfig<'a> {
    /// Lanes to create, each one an independent chain of spends.
    pub lanes: usize,
    /// Most `sendrawtransaction` calls in flight at once.
    pub max_in_flight: usize,
    /// Optional `funded_keys.json` whose keys also fund the lanes.
    pub source_keys_path: Option<&'a str>,
    /// Serialized sizes for lane spends, assigned to lanes in turn. Zero means a plain
    /// 1-in-1-out spend; empty sends only plain spends.
    pub tx_bytes: Vec<usize>,
    /// Zatoshis per ZIP 317 logical action that sized spends pay.
    pub marginal_fee: u64,
}

/// The spend one lane sends, and what it costs.
#[derive(Clone, Copy, Debug)]
struct SpendSize {
    /// `None` sends the builder's plain spend.
    target_bytes: Option<usize>,
    fee: u64,
}

impl SpendSize {
    /// The least a lane must hold to send this spend and keep a little change.
    fn min_lane_value(&self) -> u64 {
        self.fee + MIN_LANE_VALUE - transparent_fee(1, 1)
    }
}

/// One lane's current unspent output, at the blaster's address.
#[derive(Clone, Debug)]
struct Lane {
    id: u64,
    txid: String,
    output_index: u32,
    value: u64,
}

/// A submitted lane spend, waiting to be seen in a block.
struct InFlight {
    /// The output this spend consumes, which the lane spends again if the node drops it.
    spent: Lane,
    next: Lane,
    submitted_at: Instant,
    submitted_height: u32,
    /// Whether a submit task still holds this spend, so it may yet reach the node.
    sending: bool,
}

/// One signed spend being sent to the node.
struct Submission {
    txid: String,
    bytes: Arc<Vec<u8>>,
    spent: Lane,
    build_duration_ms: u64,
    /// 1 for the first send; resends of the same bytes count up from there.
    attempt: u32,
}

/// What a submit task reports back to the run loop.
struct SubmitOutcome {
    submission: Submission,
    rpc_submit_duration_ms: u64,
    result: Result<()>,
}

/// Runs the transparent load against `client` until the process is stopped.
pub async fn run(
    rpc_endpoint: &str,
    network_params: TxblastNetworkParams,
    funded_key: &FundedKey,
    rate: u64,
    config: TransparentLoadConfig<'_>,
    trace_config: &TxblastTraceConfig,
) -> Result<()> {
    if config.lanes == 0 || config.max_in_flight == 0 {
        anyhow::bail!("--transparent-lanes and --transparent-max-in-flight must be positive");
    }
    let client = Arc::new(ZebraRpcClient::new(rpc_endpoint));
    let tracer = OrchardTxblastTracer::from_config(trace_config, &funded_key.name);
    let address = transparent_address(&funded_key.address.to_string())?;
    let sizes = plan_spend_sizes(
        network_params,
        funded_key,
        &address,
        &config.tx_bytes,
        config.marginal_fee,
        client.get_block_count().await?,
    )?;
    let size_of = |lane: &Lane| sizes[lane.id as usize % sizes.len()];

    let sources = source_keys(funded_key, config.source_keys_path)?;
    let fan_utxos = fund_fan_utxos(
        &client,
        network_params,
        funded_key,
        &address,
        &sources,
        config.lanes,
    )
    .await?;
    let script = lane_script(&client, funded_key).await?;
    let script_bytes = hex::decode(&script).context("lane script is not hex")?;
    let mut ready = split_into_lanes(
        &client,
        network_params,
        funded_key,
        &address,
        &script,
        fan_utxos,
        config.lanes,
        &tracer,
    )
    .await?;
    let lane_value = ready.front().map_or(0, |lane| lane.value);
    println!(
        "[transparent] steady state ready_lanes={}, lane_value_zats={}, rate={}, max_in_flight={}",
        ready.len(),
        lane_value,
        rate,
        config.max_in_flight,
    );
    for size in sizes.iter().filter(|size| size.target_bytes.is_some()) {
        println!(
            "[transparent] lanes sized {:?} bytes afford about {} spends each",
            size.target_bytes,
            lane_value.saturating_sub(MIN_LANE_VALUE) / size.fee,
        );
    }

    let semaphore = Arc::new(Semaphore::new(config.max_in_flight));
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<SubmitOutcome>();
    let mut pending: HashMap<String, InFlight> = HashMap::new();
    // Lanes whose spend the node refused, held until the next block: a full pool
    // refuses the same spend again until its tip changes.
    let mut parked: Vec<Lane> = Vec::new();
    let mut tip = client.get_block_count().await?;
    let mut scanned = tip;
    let mut credit = 0.0f64;
    let mut last_tick = Instant::now();
    let mut ticker = tokio::time::interval(Duration::from_millis(10));
    let mut block_poll = tokio::time::interval(Duration::from_millis(250));
    let mut progress = tokio::time::interval(Duration::from_secs(10));
    let (mut submitted, mut confirmed, mut failed, mut retired, mut dropped) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    let started = Instant::now();

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let now = Instant::now();
                // Credit is capped at one second's worth, so a stall doesn't become a burst.
                credit = (credit + rate as f64 * now.duration_since(last_tick).as_secs_f64())
                    .min(rate.max(1) as f64);
                last_tick = now;
                while credit >= 1.0 && semaphore.available_permits() > 0 {
                    let Some(lane) = ready.pop_front() else { break };
                    let size = size_of(&lane);
                    if lane.value < size.min_lane_value() {
                        retired += 1;
                        continue;
                    }
                    credit -= 1.0;
                    let permit = semaphore.clone().acquire_owned().await.expect("semaphore is never closed");
                    let build_start = Instant::now();
                    let built = build_lane_spend(
                        network_params,
                        funded_key,
                        &address,
                        &script,
                        &script_bytes,
                        &lane,
                        size,
                        config.marginal_fee,
                        tip,
                    );
                    let build_duration_ms = build_start.elapsed().as_millis() as u64;
                    let (txid, bytes, next_value) = match built {
                        Ok(built) => built,
                        Err(error) => {
                            failed += 1;
                            trace(&tracer, RuntimePhase::SteadyState, tip, "tx_build_failed", None, Some(error.to_string()), None, None, None, ready.len(), pending.len());
                            continue;
                        }
                    };
                    // Recorded before sending: the block holding it may be scanned before the
                    // submit task reports back.
                    pending.insert(txid.clone(), InFlight {
                        spent: lane.clone(),
                        next: Lane { id: lane.id, txid: txid.clone(), output_index: 0, value: next_value },
                        submitted_at: Instant::now(),
                        submitted_height: tip,
                        sending: true,
                    });
                    let submission = Submission { txid, bytes: Arc::new(bytes), spent: lane, build_duration_ms, attempt: 1 };
                    spawn_submit(&client, &semaphore, Some(permit), &done_tx, submission, Duration::ZERO);
                }
            }
            Some(outcome) = done_rx.recv() => {
                let SubmitOutcome { submission, rpc_submit_duration_ms, result } = outcome;
                // A resend the node already holds, or has mined, was accepted the first time.
                let result = result.or_else(|error| if is_already_known(&error) { Ok(()) } else { Err(error) });
                let resend = matches!(&result, Err(error) if !is_node_rejection(error))
                    && submission.attempt < MAX_SUBMIT_ATTEMPTS;
                if let Some(p) = pending.get_mut(&submission.txid) {
                    p.sending = resend;
                }
                match result {
                    Ok(()) => {
                        submitted += 1;
                        trace(&tracer, RuntimePhase::SteadyState, tip, "tx_submitted", Some(&submission.txid), None, Some(submission.build_duration_ms), Some(rpc_submit_duration_ms), None, ready.len(), pending.len());
                    }
                    Err(error) => {
                        failed += 1;
                        trace(&tracer, RuntimePhase::SteadyState, tip, "tx_submit_failed", Some(&submission.txid), Some(error.to_string()), None, Some(rpc_submit_duration_ms), None, ready.len(), pending.len());
                        if is_node_rejection(&error) {
                            pending.remove(&submission.txid);
                            parked.push(submission.spent);
                        } else if resend && pending.contains_key(&submission.txid) {
                            // A timeout or unreadable reply may hide an accepted spend, so the same
                            // signed bytes go again. After the last try the spend stays pending: a
                            // block releases it, or `recover_dropped` once it's gone.
                            let retry = Submission { attempt: submission.attempt + 1, ..submission };
                            spawn_submit(&client, &semaphore, None, &done_tx, retry, RESUBMIT_DELAY);
                        }
                    }
                }
            }
            _ = block_poll.tick() => {
                let Ok(height) = client.get_block_count().await else { continue };
                while scanned < height {
                    let next = scanned + 1;
                    let Ok(txids) = block_txids(&client, next).await else { break };
                    for txid in txids {
                        if let Some(done) = pending.remove(&txid) {
                            confirmed += 1;
                            let delay = (
                                done.submitted_at.elapsed().as_millis() as u64,
                                next.saturating_sub(done.submitted_height),
                            );
                            trace(&tracer, RuntimePhase::SteadyState, next, "tx_confirmed", Some(&txid), None, None, None, Some(delay), ready.len(), pending.len());
                            if done.next.value >= size_of(&done.next).min_lane_value() {
                                ready.push_back(done.next);
                            } else {
                                retired += 1;
                            }
                        }
                    }
                    scanned = next;
                    ready.extend(parked.drain(..));
                }
                // Only once every block up to `height` is scanned, so a mined spend is
                // never mistaken for a dropped one.
                if height > tip && scanned == height {
                    dropped += recover_dropped(&client, &mut pending, &mut ready, height, &tracer).await;
                }
                tip = height;
            }
            _ = progress.tick() => {
                let secs = started.elapsed().as_secs_f64().max(1.0);
                println!(
                    "[transparent] height={} submitted={} ({:.1}/s) confirmed={} failed={} dropped={} ready={} pending={} parked={} retired={}",
                    tip, submitted, submitted as f64 / secs, confirmed, failed, dropped, ready.len(), pending.len(), parked.len(), retired,
                );
            }
        }
    }
}

/// Builds `lane`'s next spend at `size`, returning its txid, bytes and change value.
#[allow(clippy::too_many_arguments)]
fn build_lane_spend(
    network_params: TxblastNetworkParams,
    funded_key: &FundedKey,
    address: &TransparentAddress,
    script: &str,
    script_bytes: &[u8],
    lane: &Lane,
    size: SpendSize,
    marginal_fee: u64,
    tip: u32,
) -> Result<(String, Vec<u8>, u64)> {
    let plain_change = lane
        .value
        .checked_sub(transparent_fee(1, 1))
        .context("lane value cannot pay a plain spend's fee")?;
    let (txid, bytes) = build_transparent_tx(
        network_params,
        funded_key,
        &[TransparentInput {
            txid: &lane.txid,
            output_index: lane.output_index,
            script,
            value: lane.value,
        }],
        &[(*address, plain_change)],
        local_target_height(tip, 1),
    )?;
    let Some(target_bytes) = size.target_bytes else {
        return Ok((txid, bytes, plain_change));
    };
    let resized = transparent_sized::resize(
        &bytes,
        funded_key,
        lane.value,
        script_bytes,
        target_bytes,
        marginal_fee,
    )?;
    Ok((resized.txid, resized.bytes, lane.value - resized.fee))
}

/// Builds one spend at each requested size from a stand-in lane, so a bad size fails
/// before setup spends anything, and returns each size's fee.
fn plan_spend_sizes(
    network_params: TxblastNetworkParams,
    funded_key: &FundedKey,
    address: &TransparentAddress,
    tx_bytes: &[usize],
    marginal_fee: u64,
    tip: u32,
) -> Result<Vec<SpendSize>> {
    let plain = SpendSize {
        target_bytes: None,
        fee: transparent_fee(1, 1),
    };
    if tx_bytes.is_empty() {
        return Ok(vec![plain]);
    }
    // The stand-in spends an output locked to the blaster's own P2PKH script.
    let script_bytes = zcash_transparent::address::Script::from(address.script())
        .0
        .0;
    let script = hex::encode(&script_bytes);
    let stand_in = Lane {
        id: 0,
        txid: "11".repeat(32),
        output_index: 0,
        value: 1_000_000_000_000,
    };
    tx_bytes
        .iter()
        .map(|&target| {
            if target == 0 {
                return Ok(plain);
            }
            let size = SpendSize {
                target_bytes: Some(target),
                fee: 0,
            };
            let (_, bytes, change) = build_lane_spend(
                network_params,
                funded_key,
                address,
                &script,
                &script_bytes,
                &stand_in,
                size,
                marginal_fee,
                tip,
            )
            .with_context(|| format!("--transparent-tx-bytes {target}"))?;
            let fee = stand_in.value - change;
            println!(
                "[transparent] spend size target={target} bytes={} fee_zats={fee}",
                bytes.len()
            );
            Ok(SpendSize { fee, ..size })
        })
        .collect()
}

/// Returns the blaster's key plus any distinct keys in `path`.
fn source_keys(funded_key: &FundedKey, path: Option<&str>) -> Result<Vec<FundedKey>> {
    let mut keys = vec![funded_key.clone()];
    let Some(path) = path else {
        return Ok(keys);
    };
    let raw: Vec<LocalGenesisFundedKey> = serde_json::from_slice(
        &std::fs::read(path).with_context(|| format!("failed to read {path}"))?,
    )
    .with_context(|| format!("failed to parse {path}"))?;
    let mut seen: HashSet<String> = HashSet::from([funded_key.address.to_string()]);
    for raw in raw {
        let key = funded_key_from_raw(raw)?;
        if seen.insert(key.address.to_string()) {
            keys.push(key);
        }
    }
    Ok(keys)
}

/// Shields every source key's mature funds and unshields them into fan UTXOs at
/// `address`, returning those UTXOs as `(txid, output_index, value)`.
async fn fund_fan_utxos(
    client: &ZebraRpcClient,
    network_params: TxblastNetworkParams,
    funded_key: &FundedKey,
    address: &TransparentAddress,
    sources: &[FundedKey],
    lanes: usize,
) -> Result<Vec<(String, u32, u64)>> {
    let fans_per_source = lanes.div_ceil(FAN_OUTPUTS).div_ceil(sources.len()).max(1);
    let min_fan_value = transparent_fee(1, FAN_OUTPUTS) + FAN_OUTPUTS as u64 * MIN_LANE_VALUE;
    let tip = client.get_block_count().await?;
    let funding = futures::future::join_all(sources.iter().map(|source| async move {
        let utxos = select_spendable_treasury_utxos(client, source, tip, MAX_SOURCE_UTXOS).await?;
        shield_and_unshield(
            client,
            network_params,
            source,
            &utxos,
            &vec![*address; fans_per_source],
            min_fan_value,
            SETUP_CONFIRM_TIMEOUT,
            "[transparent]",
        )
        .await
    }))
    .await;

    let mut funding_txids = HashSet::new();
    for (source, result) in sources.iter().zip(funding) {
        match result {
            Ok(funded) => {
                funding_txids.insert(funded.funding_txid);
            }
            // A source with too little mature value is skipped; the rest still fund lanes.
            Err(error) => println!("[transparent] skipping source {}: {error:#}", source.name),
        }
    }
    if funding_txids.is_empty() {
        anyhow::bail!("no source key could fund transparent lanes");
    }
    let fans: Vec<(String, u32, u64)> = client
        .get_address_utxos(&funded_key.address.to_string())
        .await?
        .into_iter()
        .filter(|utxo| funding_txids.contains(&utxo.txid))
        .map(|utxo| (utxo.txid, utxo.output_index, utxo.satoshis))
        .collect();
    println!(
        "[transparent] funded {} fan UTXOs worth {} zats from {} of {} source keys",
        fans.len(),
        fans.iter().map(|fan| fan.2).sum::<u64>(),
        funding_txids.len(),
        sources.len(),
    );
    Ok(fans)
}

/// Splits each fan UTXO into up to [`FAN_OUTPUTS`] equal lanes, waits for every split
/// to confirm, and returns the lanes. Stops once `lanes` lanes exist.
#[allow(clippy::too_many_arguments)]
async fn split_into_lanes(
    client: &ZebraRpcClient,
    network_params: TxblastNetworkParams,
    funded_key: &FundedKey,
    address: &TransparentAddress,
    script: &str,
    fans: Vec<(String, u32, u64)>,
    lanes: usize,
    tracer: &OrchardTxblastTracer,
) -> Result<VecDeque<Lane>> {
    let tip = client.get_block_count().await?;
    let mut created: VecDeque<Lane> = VecDeque::new();
    let mut waiting: HashSet<String> = HashSet::new();
    for (txid, output_index, value) in fans {
        let outputs = (lanes - created.len()).min(FAN_OUTPUTS);
        if outputs == 0 {
            break;
        }
        let spendable = value - transparent_fee(1, outputs);
        let each = spendable / outputs as u64;
        if each < MIN_LANE_VALUE {
            println!(
                "[transparent] fan UTXO {txid}:{output_index} is too small for {outputs} lanes"
            );
            continue;
        }
        // The first output takes the remainder, so outputs plus fee equal the input exactly.
        let mut values = vec![each; outputs];
        values[0] += spendable - each * outputs as u64;
        let recipients: Vec<(TransparentAddress, u64)> =
            values.iter().map(|v| (*address, *v)).collect();
        let (split_txid, bytes) = build_transparent_tx(
            network_params,
            funded_key,
            &[TransparentInput {
                txid: &txid,
                output_index,
                script,
                value,
            }],
            &recipients,
            local_target_height(tip, 10),
        )?;
        client.send_raw_transaction(&hex::encode(&bytes)).await?;
        trace(
            tracer,
            RuntimePhase::BootstrapShield,
            tip,
            "tx_submitted",
            Some(&split_txid),
            None,
            None,
            None,
            None,
            created.len(),
            waiting.len(),
        );
        for (index, value) in values.into_iter().enumerate() {
            created.push_back(Lane {
                id: created.len() as u64,
                txid: split_txid.clone(),
                output_index: index as u32,
                value,
            });
        }
        waiting.insert(split_txid);
    }

    let deadline = Instant::now() + SETUP_CONFIRM_TIMEOUT;
    let mut scanned = tip;
    while !waiting.is_empty() {
        if Instant::now() >= deadline {
            anyhow::bail!("{} lane split transactions never confirmed", waiting.len());
        }
        let height = client.get_block_count().await?;
        while scanned < height {
            scanned += 1;
            for txid in block_txids(client, scanned).await? {
                waiting.remove(&txid);
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Ok(created)
}

/// Returns the P2PKH script the lanes are locked to, from any UTXO at the key's address.
async fn lane_script(client: &ZebraRpcClient, funded_key: &FundedKey) -> Result<String> {
    client
        .get_address_utxos(&funded_key.address.to_string())
        .await?
        .into_iter()
        .next()
        .map(|utxo| utxo.script)
        .context("the blaster address has no UTXO to read its script from")
}

/// Returns lanes whose spend has gone `STALE_BLOCKS` unmined and is no longer in the
/// node's pool to `ready`, and returns how many it recovered.
///
/// `height` must be the last block the caller scanned. The blaster submits to the only
/// mining node, so a spend that node evicted or let expire won't be mined, and its
/// input is still the lane's to spend. If a block arrives while the pool is read, a
/// spend missing from the pool may be in that block, so nothing is recovered until the
/// next scan.
async fn recover_dropped(
    client: &ZebraRpcClient,
    pending: &mut HashMap<String, InFlight>,
    ready: &mut VecDeque<Lane>,
    height: u32,
    tracer: &OrchardTxblastTracer,
) -> u64 {
    if !pending
        .values()
        .any(|p| p.submitted_height + STALE_BLOCKS <= height)
    {
        return 0;
    }
    let Ok(pool) = client
        .call_raw("getrawmempool", serde_json::json!([]))
        .await
    else {
        return 0;
    };
    if client.get_block_count().await.ok() != Some(height) {
        return 0;
    }
    let pool: HashSet<&str> = pool
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|t| t.as_str())
        .collect();
    let stale = dropped_spends(pending, &pool, height);
    for txid in &stale {
        let dropped = pending
            .remove(txid)
            .expect("txid was just read from pending");
        ready.push_back(dropped.spent);
        trace(
            tracer,
            RuntimePhase::SteadyState,
            height,
            "pending_tx_evicted",
            Some(txid),
            None,
            None,
            None,
            None,
            ready.len(),
            pending.len(),
        );
    }
    stale.len() as u64
}

/// Sends `submission` to the node after `delay`, holding `permit`, or a fresh one from
/// `semaphore` when none is given, while the call is in flight.
fn spawn_submit(
    client: &Arc<ZebraRpcClient>,
    semaphore: &Arc<Semaphore>,
    permit: Option<OwnedSemaphorePermit>,
    done_tx: &mpsc::UnboundedSender<SubmitOutcome>,
    submission: Submission,
    delay: Duration,
) {
    let (client, semaphore, done_tx) = (client.clone(), semaphore.clone(), done_tx.clone());
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        let permit = match permit {
            Some(permit) => permit,
            None => semaphore
                .acquire_owned()
                .await
                .expect("semaphore is never closed"),
        };
        let rpc_start = Instant::now();
        let result = client
            .send_raw_transaction(&hex::encode(submission.bytes.as_slice()))
            .await
            .map(|_| ());
        drop(permit);
        let _ = done_tx.send(SubmitOutcome {
            submission,
            rpc_submit_duration_ms: rpc_start.elapsed().as_millis() as u64,
            result,
        });
    });
}

/// Returns whether `error` says the node already holds or has mined this transaction.
fn is_already_known(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    [
        "already exists in mempool",
        "already queued",
        "already in state",
    ]
    .iter()
    .any(|known| message.contains(known))
}

/// Returns whether `error` is the node refusing the spend, as opposed to a timeout,
/// dropped connection or unreadable reply that can't say whether the node took it.
fn is_node_rejection(error: &anyhow::Error) -> bool {
    error.to_string().starts_with("RPC error from ")
}

/// Returns the pending spends submitted at least `STALE_BLOCKS` before `height` that
/// the node's `pool` no longer holds and no submit task can still send.
fn dropped_spends(
    pending: &HashMap<String, InFlight>,
    pool: &HashSet<&str>,
    height: u32,
) -> Vec<String> {
    pending
        .iter()
        .filter(|(txid, p)| {
            !p.sending
                && p.submitted_height + STALE_BLOCKS <= height
                && !pool.contains(txid.as_str())
        })
        .map(|(txid, _)| txid.clone())
        .collect()
}

/// Returns the txids of the block at `height`.
async fn block_txids(client: &ZebraRpcClient, height: u32) -> Result<Vec<String>> {
    let block = client
        .call_raw("getblock", serde_json::json!([height.to_string(), 1]))
        .await?;
    Ok(block["tx"]
        .as_array()
        .context("getblock verbosity 1 has no tx list")?
        .iter()
        .filter_map(|txid| txid.as_str().map(ToOwned::to_owned))
        .collect())
}

/// Decodes an encoded transparent address.
fn transparent_address(encoded: &str) -> Result<TransparentAddress> {
    ZcashAddress::try_from_encoded(encoded)
        .with_context(|| format!("invalid transparent address {encoded}"))?
        .convert::<TransparentAddress>()
        .map_err(|e| anyhow::anyhow!("address {encoded} is not transparent: {e:?}"))
}

/// Writes one event in the Orchard mode's schema.
#[allow(clippy::too_many_arguments)]
fn trace(
    tracer: &OrchardTxblastTracer,
    phase: RuntimePhase,
    height: u32,
    event: &'static str,
    txid: Option<&str>,
    error: Option<String>,
    build_duration_ms: Option<u64>,
    rpc_submit_duration_ms: Option<u64>,
    confirm: Option<(u64, u32)>,
    ready_lanes: usize,
    pending: usize,
) {
    tracer.trace_event(EventContext {
        height: Some(height),
        phase,
        event,
        tx_kind: None,
        txid,
        lane_id: None,
        note_id: None,
        note_role: None,
        note_value: None,
        pending: PendingTxCounts {
            total: pending,
            ..Default::default()
        },
        registry: RegistrySnapshot {
            ready_lanes,
            ..Default::default()
        },
        treasury: TreasurySnapshot::default(),
        reason: Some("transparent_lane"),
        error,
        error_class: None,
        build_duration_ms,
        rpc_submit_duration_ms,
        confirm_delay_ms: confirm.map(|c| c.0),
        confirm_delay_blocks: confirm.map(|c| c.1),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn in_flight(submitted_height: u32) -> InFlight {
        let lane = Lane {
            id: 0,
            txid: "in".into(),
            output_index: 0,
            value: 50_000,
        };
        InFlight {
            spent: lane.clone(),
            next: lane,
            submitted_at: Instant::now(),
            submitted_height,
            sending: false,
        }
    }

    #[test]
    fn only_stale_spends_missing_from_the_pool_are_dropped() {
        let still_sending = InFlight {
            sending: true,
            ..in_flight(10)
        };
        let pending: HashMap<String, InFlight> = [
            ("evicted".to_string(), in_flight(10)),
            ("still-pooled".to_string(), in_flight(10)),
            ("recent".to_string(), in_flight(12)),
            ("still-sending".to_string(), still_sending),
        ]
        .into_iter()
        .collect();
        let pool: HashSet<&str> = ["still-pooled"].into_iter().collect();
        assert_eq!(
            dropped_spends(&pending, &pool, 10 + STALE_BLOCKS),
            vec!["evicted".to_string()]
        );
    }

    #[test]
    fn spend_sizes_price_each_target_and_reject_sizes_without_room() {
        let (key, address, _) = crate::txblast::transparent_sized::tests::key();
        let plan = |sizes: &[usize]| {
            plan_spend_sizes(
                TxblastNetworkParams::LocalGenesis,
                &key,
                &address,
                sizes,
                5_000,
                100,
            )
        };
        let plain = plan(&[]).unwrap();
        assert_eq!(plain.len(), 1);
        assert_eq!(plain[0].target_bytes, None);
        assert_eq!(plain[0].min_lane_value(), MIN_LANE_VALUE);

        let mixed = plan(&[0, 300, 100_000]).unwrap();
        assert_eq!(mixed[0].target_bytes, None);
        assert_eq!(mixed[0].fee, transparent_fee(1, 1));
        // ZIP 317 charges one action per 34 output bytes. The 300-byte spend's outputs
        // take 4 actions; the 100,000-byte spend's take 2,937.
        assert_eq!(mixed[1].fee, 4 * 5_000);
        assert_eq!(mixed[2].fee, 2_937 * 5_000);
        assert_eq!(mixed[2].min_lane_value(), mixed[2].fee + 10_000);

        assert!(plan(&[100]).is_err());
    }

    #[test]
    fn only_a_node_error_reply_counts_as_a_rejection() {
        let refused = anyhow::anyhow!("RPC error from sendrawtransaction: mempool is full");
        let unreadable = anyhow::anyhow!("bad body").context("failed to parse RPC response");
        assert!(is_node_rejection(&refused));
        assert!(!is_node_rejection(&unreadable));
    }

    #[test]
    fn a_resend_the_node_already_holds_counts_as_accepted() {
        let held = anyhow::anyhow!(
            "RPC error from sendrawtransaction: transaction already exists in mempool"
        );
        let full = anyhow::anyhow!("RPC error from sendrawtransaction: mempool is full");
        assert!(is_already_known(&held));
        assert!(!is_already_known(&full));
    }
}
