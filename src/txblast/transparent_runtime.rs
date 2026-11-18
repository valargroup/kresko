//! Confirmed-input workload generator for private experiment chains.
use std::{
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{task::JoinSet, time::MissedTickBehavior};

use super::{
    rpc::{AddressUtxo, ZebraRpcClient},
    transparent::load_funded_key,
    transparent_builder,
};

#[derive(clap::Args, Debug)]
pub struct Options {
    #[arg(long)]
    pub rpc_endpoint: String,
    #[arg(long)]
    pub funded_key_path: Option<String>,
    /// Persistent reservation journal. Each wallet must use one journal.
    #[arg(long)]
    pub journal: PathBuf,
    /// Maximum new transactions per second, including fanout transactions.
    #[arg(long, default_value_t = 12)]
    pub rate: u32,
    /// Target number of confirmed UTXO lanes, including reserved inputs.
    #[arg(long, default_value_t = 2048)]
    pub lanes: usize,
    /// Maximum concurrent signing and submission tasks.
    #[arg(long, default_value_t = 16)]
    pub concurrency: usize,
    /// OP_RETURN bytes per self-spend (0 disables the output).
    #[arg(long, default_value_t = 23)]
    pub null_data_bytes: usize,
    /// Maximum unconfirmed reservations plus signing tasks; zero disables this cap.
    #[arg(long, default_value_t = 0)]
    pub max_pending_transactions: usize,
    #[arg(long, default_value_t = 5.0)]
    pub refresh_secs: f64,
    /// Exit after this many seconds; zero runs until interrupted.
    #[arg(long, default_value_t = 0)]
    pub duration_secs: u64,
    /// Optional JSONL event log. The console always reports progress.
    #[arg(long)]
    pub trace: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Pending {
    txid: String,
    input: String,
    raw_hex: String,
    expiry_height: u32,
    output_count: usize,
    fee: u64,
    bytes: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum Record {
    Identity {
        version: u32,
        genesis: String,
        address: String,
    },
    Reserve {
        transaction: Pending,
    },
    ExpiryFloor {
        height: u32,
    },
    Clear {
        txid: String,
    },
}

struct Journal {
    path: PathBuf,
    file: File,
    _lock: File,
    identity: Record,
    records: usize,
    expiry_floor: Option<u32>,
}

impl Journal {
    fn open(
        path: &Path,
        genesis: String,
        address: String,
    ) -> Result<(Self, HashMap<String, Pending>)> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let mut lock_name = path.as_os_str().to_owned();
        lock_name.push(".lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(lock_name)?;
        lock.try_lock()
            .context("another transparent generator holds the journal lock")?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .append(true)
            .open(path)?;
        let mut pending = HashMap::new();
        let mut reader = BufReader::new(file.try_clone()?);
        let mut good_len = 0;
        let mut records = 0;
        let mut found_identity = false;
        let mut expiry_floor: Option<u32> = None;
        loop {
            let mut line = Vec::new();
            let n = reader.read_until(b'\n', &mut line)?;
            if n == 0 {
                break;
            }
            // An interrupted append can leave a partial final record. Nothing was
            // submitted until that append reached durable storage.
            if line.last() != Some(&b'\n') {
                break;
            }
            let record: Record =
                serde_json::from_slice(&line).context("corrupt reservation journal")?;
            match record {
                Record::Identity {
                    version,
                    genesis: saved_genesis,
                    address: saved_address,
                } => {
                    ensure!(
                        !found_identity && records == 0,
                        "duplicate journal identity"
                    );
                    ensure!(
                        version == 1 && saved_genesis == genesis && saved_address == address,
                        "journal belongs to a different chain or wallet"
                    );
                    found_identity = true;
                }
                Record::Reserve { transaction } => {
                    ensure!(found_identity, "journal has no identity");
                    pending.insert(transaction.txid.clone(), transaction);
                }
                Record::ExpiryFloor { height } => {
                    ensure!(found_identity, "journal has no identity");
                    expiry_floor = Some(expiry_floor.map_or(height, |old| old.max(height)));
                }
                Record::Clear { txid } => {
                    ensure!(found_identity, "journal has no identity");
                    pending.remove(&txid);
                }
            }
            good_len += n as u64;
            records += 1;
        }
        file.set_len(good_len)?;
        let identity = Record::Identity {
            version: 1,
            genesis,
            address,
        };
        if !found_identity {
            write_record(&mut file, &identity)?;
            file.sync_all()?;
            File::open(
                path.parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new(".")),
            )?
            .sync_all()?;
        }
        Ok((
            Self {
                path: path.to_owned(),
                file,
                _lock: lock,
                identity,
                records,
                expiry_floor,
            },
            pending,
        ))
    }

    fn append(&mut self, record: &Record) -> Result<()> {
        write_record(&mut self.file, record)?;
        self.file.sync_data()?;
        self.records += 1;
        Ok(())
    }

    fn clear(&mut self, txids: &[String]) -> Result<()> {
        if txids.is_empty() {
            return Ok(());
        }
        for txid in txids {
            write_record(&mut self.file, &Record::Clear { txid: txid.clone() })?;
        }
        self.file.sync_data()?;
        self.records += txids.len();
        Ok(())
    }

    fn check_height(&self, height: u32) -> Result<()> {
        ensure!(
            self.expiry_floor.is_none_or(|floor| height > floor),
            "chain reorg crossed discarded transaction expiry; stop and reconcile this wallet before using a new journal"
        );
        Ok(())
    }

    fn prune_expiry(&mut self, height: u32) -> Result<()> {
        if self.expiry_floor.is_none_or(|old| height > old) {
            self.append(&Record::ExpiryFloor { height })?;
            self.expiry_floor = Some(height);
        }
        Ok(())
    }

    fn compact(&mut self, pending: &HashMap<String, Pending>) -> Result<()> {
        if self.records < 10_000 {
            return Ok(());
        }
        let mut temp_name = self.path.as_os_str().to_owned();
        temp_name.push(".new");
        let mut replacement = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp_name)?;
        write_record(&mut replacement, &self.identity)?;
        if let Some(height) = self.expiry_floor {
            write_record(&mut replacement, &Record::ExpiryFloor { height })?;
        }
        for transaction in pending.values() {
            write_record(
                &mut replacement,
                &Record::Reserve {
                    transaction: transaction.clone(),
                },
            )?;
        }
        replacement.sync_all()?;
        std::fs::rename(&temp_name, &self.path)?;
        File::open(
            self.path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )?
        .sync_all()?;
        self.file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.path)?;
        self.records = 0;
        Ok(())
    }
}

fn write_record(file: &mut File, record: &Record) -> Result<()> {
    let mut bytes = serde_json::to_vec(record)?;
    bytes.push(b'\n');
    file.write_all(&bytes)?;
    Ok(())
}

fn outpoint(utxo: &AddressUtxo) -> String {
    format!("{}:{}", utxo.txid, utxo.output_index)
}

struct Snapshot {
    height: u32,
    utxos: HashMap<String, AddressUtxo>,
    mempool: HashSet<String>,
    coinbase_utxos: usize,
}

async fn snapshot(
    client: &ZebraRpcClient,
    address: &str,
    pending: &HashMap<String, Pending>,
    coinbase_cache: &mut HashMap<String, bool>,
) -> Result<Snapshot> {
    let before = client.get_blockchain_info().await?;
    let height = u32::try_from(before["blocks"].as_u64().context("missing chain height")?)?;
    let hash = before["bestblockhash"]
        .as_str()
        .context("missing chain tip hash")?;
    let utxos = client.get_address_utxos(address).await?;
    let mempool: HashSet<String> =
        serde_json::from_value(client.call_raw("getrawmempool", json!([])).await?)?;
    let mut usable = HashMap::new();
    let mut coinbase_utxos = 0;
    let txids: HashSet<_> = utxos.iter().map(|u| u.txid.clone()).collect();
    coinbase_cache.retain(|id, _| txids.contains(id));
    for utxo in utxos {
        if utxo.height == 0 || utxo.height > height {
            continue;
        }
        let coinbase = if pending.contains_key(&utxo.txid) {
            false
        } else if let Some(value) = coinbase_cache.get(&utxo.txid) {
            *value
        } else {
            let tx = client.get_raw_transaction_verbose(&utxo.txid).await?;
            let value = tx.vin.iter().any(|input| input.coinbase.is_some());
            coinbase_cache.insert(utxo.txid.clone(), value);
            value
        };
        if coinbase {
            coinbase_utxos += 1;
        } else {
            usable.insert(outpoint(&utxo), utxo);
        }
    }
    ensure!(
        client.get_best_block_hash().await? == hash,
        "chain tip changed during UTXO snapshot; retrying"
    );
    Ok(Snapshot {
        height,
        utxos: usable,
        mempool,
        coinbase_utxos,
    })
}

fn projected_lanes(
    snapshot: &Snapshot,
    pending: &HashMap<String, Pending>,
    minimum_input: u64,
) -> usize {
    snapshot
        .utxos
        .values()
        .filter(|u| u.satoshis >= minimum_input)
        .count()
        + pending
            .values()
            .filter(|tx| snapshot.utxos.contains_key(&tx.input))
            .map(|tx| tx.output_count.saturating_sub(1))
            .sum::<usize>()
}

fn expired(tx: &Pending, snapshot: &Snapshot) -> bool {
    // Keep confirmed reservations until expiry to protect their inputs if a
    // reorg restores them. Their confirmed outputs remain available immediately.
    snapshot.height > tx.expiry_height && !snapshot.mempool.contains(&tx.txid)
}

struct Events(Option<File>);
impl Events {
    fn emit(&mut self, mut value: serde_json::Value) -> Result<()> {
        value["unix_ms"] = json!(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis()
        );
        if let Some(file) = &mut self.0 {
            serde_json::to_writer(&mut *file, &value)?;
            file.write_all(b"\n")?;
        }
        Ok(())
    }
}

pub async fn run(options: Options) -> Result<()> {
    ensure!(
        options.rate > 0 && options.rate <= 100_000,
        "rate must be in 1..=100000"
    );
    ensure!(
        (1..=65536).contains(&options.lanes),
        "lanes must be in 1..=65536"
    );
    ensure!(
        (1..=256).contains(&options.concurrency),
        "concurrency must be in 1..=256"
    );
    ensure!(
        options.null_data_bytes <= 80,
        "null-data-bytes must be <=80"
    );
    let refresh_interval = Duration::try_from_secs_f64(options.refresh_secs)
        .context("refresh-secs must be a finite non-negative duration")?;
    ensure!(!refresh_interval.is_zero(), "refresh-secs must be positive");
    let minimum_input = transparent_builder::self_spend_fee(options.null_data_bytes)?
        + transparent_builder::MIN_OUTPUT_ZATS;
    let (key, _) = load_funded_key(options.funded_key_path.as_deref())?;
    let address = key.address.to_string();
    let key = Arc::new(key);
    let client = Arc::new(ZebraRpcClient::new(&options.rpc_endpoint));
    let genesis = client.get_block_hash(0).await?;
    let (mut journal, mut pending) = Journal::open(&options.journal, genesis, address.clone())?;
    let mut events = Events(
        options
            .trace
            .as_ref()
            .map(|path| OpenOptions::new().create(true).append(true).open(path))
            .transpose()?,
    );
    let mut coinbase_cache = HashMap::new();
    let mut state = None;
    for attempt in 0..5 {
        match snapshot(&client, &address, &pending, &mut coinbase_cache).await {
            Ok(fresh) => {
                state = Some(fresh);
                break;
            }
            Err(error) if attempt < 4 => {
                eprintln!("initial transparent snapshot unavailable: {error:#}; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(error.context("initial transparent UTXO snapshot")),
        }
    }
    let mut state = state.context("initial snapshot unavailable")?;
    journal.check_height(state.height)?;
    let mut signing: JoinSet<(String, Result<transparent_builder::Built>)> = JoinSet::new();
    let mut submitting = JoinSet::new();
    let mut building_inputs = HashSet::new();
    let mut submitting_ids = HashSet::new();
    let mut last_attempt: HashMap<String, Instant> = HashMap::new();
    let mut ready_inputs: BinaryHeap<(u64, String)> = BinaryHeap::new();
    let mut retries: VecDeque<String> = VecDeque::new();
    let mut projected = projected_lanes(&state, &pending, minimum_input);
    let mut accepted_ids = HashSet::new();
    let mut accepted = 0u64;
    let mut accepted_bytes = 0u64;
    let mut failures = 0u64;
    let started = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / f64::from(options.rate)));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut refresh = tokio::time::interval(refresh_interval);
    refresh.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let stop = tokio::time::sleep(Duration::from_secs(if options.duration_secs == 0 {
        100 * 365 * 24 * 60 * 60
    } else {
        options.duration_secs
    }));
    tokio::pin!(stop);
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    println!(
        "transparent txblast address={address} rate={}/s lanes={} journal={}",
        options.rate,
        options.lanes,
        options.journal.display()
    );
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = &mut stop => break,
            Some(result) = signing.join_next(), if !signing.is_empty() => {
                let (input, built) = result.context("transparent signing task panicked")?;
                building_inputs.remove(&input);
                match built {
                    Ok(built) => {
                        let tx = self::Pending { txid: built.txid, input, raw_hex: built.raw_hex, expiry_height: built.expiry_height, output_count: built.output_count, fee: built.fee, bytes: built.bytes };
                        journal.append(&Record::Reserve { transaction: tx.clone() })?;
                        events.emit(json!({"event":"built", "txid":tx.txid, "bytes":tx.bytes, "fee":tx.fee, "outputs":tx.output_count}))?;
                        if state.utxos.contains_key(&tx.input) {
                            projected += tx.output_count.saturating_sub(1);
                        }
                        pending.insert(tx.txid.clone(), tx.clone());
                        submit(&client, tx, &mut submitting, &mut submitting_ids, &mut last_attempt);
                    }
                    Err(error) => { return Err(error.context("building transparent transaction")); }
                }
            }
            Some(result) = submitting.join_next(), if !submitting.is_empty() => {
                let (txid, result) = result.context("transparent submission task panicked")?;
                submitting_ids.remove(&txid);
                match result {
                    Ok(returned) if returned == txid => {
                        if accepted_ids.insert(txid.clone()) {
                            accepted += 1;
                            accepted_bytes += pending.get(&txid).map_or(0, |tx| tx.bytes as u64);
                        }
                        events.emit(json!({"event":"accepted", "txid":txid}))?;
                    }
                    other => {
                        failures += 1;
                        events.emit(json!({"event":"submission_uncertain", "txid":txid, "result":format!("{other:?}")}))?;
                    }
                }
            }
            _ = refresh.tick() => {
                match snapshot(&client, &address, &pending, &mut coinbase_cache).await {
                    Ok(fresh) => state = fresh,
                    Err(error) => {
                        events.emit(json!({"event":"snapshot_error", "error":format!("{error:#}")}))?;
                        eprintln!("transparent snapshot unavailable: {error:#}");
                        // Never construct more transactions from a stale snapshot.
                        state.utxos.clear();
                        ready_inputs.clear();
                        retries.clear();
                        continue;
                    }
                }
                journal.check_height(state.height)?;
                let clear: Vec<_> = pending.values().filter(|tx| expired(tx, &state) && !submitting_ids.contains(&tx.txid)).map(|tx| tx.txid.clone()).collect();
                if let Some(expiry) = clear.iter().map(|id| pending[id].expiry_height).max() {
                    journal.prune_expiry(expiry)?;
                }
                journal.clear(&clear)?;
                for txid in clear {
                    pending.remove(&txid);
                    last_attempt.remove(&txid);
                    accepted_ids.remove(&txid);
                }
                journal.compact(&pending)?;
                projected = projected_lanes(&state, &pending, minimum_input);
                let reserved: HashSet<_> = pending.values().map(|tx| &tx.input).collect();
                ready_inputs = state.utxos.iter()
                    .filter(|(id, u)| !reserved.contains(id) && !building_inputs.contains(*id) && u.satoshis >= minimum_input)
                    .map(|(id, u)| (u.satoshis, id.clone())).collect();
                retries = pending.values().filter(|tx| {
                    state.utxos.contains_key(&tx.input) && !state.mempool.contains(&tx.txid)
                        && state.height <= tx.expiry_height && !submitting_ids.contains(&tx.txid)
                        && last_attempt.get(&tx.txid).is_none_or(|at| at.elapsed() >= Duration::from_secs(15))
                }).map(|tx| tx.txid.clone()).collect();
                let confirmed = pending.values().filter(|tx| state.utxos.contains_key(&format!("{}:0", tx.txid))).count();
                let progress = json!({"event":"progress", "elapsed_secs":started.elapsed().as_secs_f64(), "height":state.height, "utxos":state.utxos.len(), "projected_lanes":projected, "ready_inputs":ready_inputs.len(), "coinbase_utxos":state.coinbase_utxos, "journal_pending":pending.len(), "confirmed_outputs":confirmed, "mempool":state.mempool.len(), "accepted":accepted, "accepted_bytes":accepted_bytes, "submission_failures":failures, "signing":signing.len(), "submitting":submitting.len()});
                println!("{progress}");
                events.emit(progress)?;
                if projected < options.lanes && !state.utxos.is_empty() && state.utxos.values().all(|u| u.satoshis < 90_000) {
                    events.emit(json!({"event":"funding_needed", "reason":"lane_target_unreachable", "projected_lanes":projected, "target_lanes":options.lanes}))?;
                    eprintln!("transparent lane target needs more funding: projected={projected} target={}; existing outputs cannot split into more usable lanes", options.lanes);
                }
                if !state.utxos.is_empty() && state.utxos.values().all(|u| u.satoshis < minimum_input) {
                    events.emit(json!({"event":"funding_needed", "reason":"fee_exhausted", "utxos":state.utxos.len()}))?;
                    eprintln!("transparent fees exhausted: all confirmed outputs are below the {minimum_input}-zat minimum spend value; add non-coinbase funds");
                }
                if state.utxos.is_empty() { eprintln!("transparent funding required: provide confirmed non-coinbase outputs; shield treasury coinbases and unshield with fund-runtime-keys-local"); }
            }
            _ = tick.tick() => {
                if signing.len() + submitting.len() >= options.concurrency { continue; }
                // Retry the same signed bytes after ambiguous submission outcomes.
                if let Some(txid) = retries.pop_front() {
                    if let Some(tx) = pending.get(&txid) {
                        submit(&client, tx.clone(), &mut submitting, &mut submitting_ids, &mut last_attempt);
                        continue;
                    }
                }
                let unconfirmed = pending.values().filter(|tx| {
                    state.utxos.contains_key(&tx.input) || state.mempool.contains(&tx.txid)
                }).count();
                if options.max_pending_transactions != 0
                    && unconfirmed + signing.len() >= options.max_pending_transactions {
                    continue;
                }
                let deficit = options.lanes.saturating_sub(projected);
                // Finish a fanout reservation before choosing the next fanout.
                // Do not burn bootstrap funds on concurrent self-spends.
                if deficit > 0 && !building_inputs.is_empty() { continue; }
                let Some((_, input_id)) = ready_inputs.pop() else { continue; };
                let Some(input) = state.utxos.get(&input_id).cloned() else { continue; };
                let outputs = (deficit + 1).min(2048).min((input.satoshis / 45_000) as usize).max(1);
                building_inputs.insert(input_id.clone());
                let key = key.clone();
                let height = state.height;
                let data_bytes = if outputs > 1 { 0 } else { options.null_data_bytes };
                signing.spawn_blocking(move || (input_id, transparent_builder::build(&key, &input, height, outputs, data_bytes)));
            }
        }
    }
    // Submitted transactions retain reservations even if shutdown interrupts RPC.
    signing.abort_all();
    submitting.abort_all();
    events.emit(json!({"event":"stopped", "accepted":accepted, "accepted_bytes":accepted_bytes, "pending":pending.len()}))?;
    println!(
        "transparent txblast stopped: accepted={accepted} accepted_bytes={accepted_bytes} reservations={}",
        pending.len()
    );
    Ok(())
}

type Submissions = JoinSet<(String, Result<String>)>;
fn submit(
    client: &Arc<ZebraRpcClient>,
    tx: Pending,
    tasks: &mut Submissions,
    active: &mut HashSet<String>,
    attempts: &mut HashMap<String, Instant>,
) {
    active.insert(tx.txid.clone());
    attempts.insert(tx.txid.clone(), Instant::now());
    let client = client.clone();
    tasks.spawn(async move {
        let result = client
            .call_raw("sendrawtransaction", json!([tx.raw_hex]))
            .await
            .and_then(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .context("sendrawtransaction returned no txid")
            });
        (tx.txid, result)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    fn pending(id: &str, input: &str, outputs: usize) -> Pending {
        Pending {
            txid: id.into(),
            input: input.into(),
            raw_hex: "00".into(),
            expiry_height: 100,
            output_count: outputs,
            fee: 20_000,
            bytes: 301,
        }
    }
    fn snapshot_at(height: u32) -> Snapshot {
        Snapshot {
            height,
            utxos: HashMap::new(),
            mempool: HashSet::new(),
            coinbase_utxos: 0,
        }
    }
    fn scratch() -> PathBuf {
        let root = std::env::var_os("TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let dir = root.join(format!(
            "kresko-transparent-test-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
    #[test]
    fn journal_recovers_reservations_and_rejects_other_wallets() {
        let dir = scratch();
        let path = dir.join("journal.jsonl");
        let (mut journal, empty) = Journal::open(&path, "genesis".into(), "wallet".into()).unwrap();
        assert!(empty.is_empty());
        journal
            .append(&Record::Reserve {
                transaction: pending("a", "input:0", 2),
            })
            .unwrap();
        assert!(Journal::open(&path, "genesis".into(), "wallet".into()).is_err());
        drop(journal);
        let (mut journal, saved) = Journal::open(&path, "genesis".into(), "wallet".into()).unwrap();
        assert_eq!(saved["a"].input, "input:0");
        journal.records = 10_000;
        journal.compact(&saved).unwrap();
        journal.append(&Record::Clear { txid: "a".into() }).unwrap();
        drop(journal);
        assert!(Journal::open(&path, "other".into(), "wallet".into()).is_err());
        assert!(Journal::open(&path, "genesis".into(), "other".into()).is_err());
        let (journal, saved) = Journal::open(&path, "genesis".into(), "wallet".into()).unwrap();
        assert!(saved.is_empty());
        drop(journal);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn journal_discards_only_an_incomplete_final_record() {
        let dir = scratch();
        let path = dir.join("journal.jsonl");
        let (mut journal, _) = Journal::open(&path, "g".into(), "a".into()).unwrap();
        journal
            .append(&Record::Reserve {
                transaction: pending("a", "input:0", 1),
            })
            .unwrap();
        journal.file.write_all(b"{\"event\":").unwrap();
        drop(journal);
        let (journal, saved) = Journal::open(&path, "g".into(), "a".into()).unwrap();
        assert!(saved.contains_key("a"));
        drop(journal);
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"not valid json\n")
            .unwrap();
        assert!(Journal::open(&path, "g".into(), "a".into()).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn confirmation_does_not_release_a_reservation_before_expiry() {
        let tx = pending("a", "input:0", 1);
        assert!(!expired(&tx, &snapshot_at(100)));
        assert!(expired(&tx, &snapshot_at(101)));
        let mut state = snapshot_at(101);
        state.mempool.insert("a".into());
        assert!(!expired(&tx, &state));
        // A reorg restores an input while the signed transaction remains valid.
        assert!(!expired(&tx, &snapshot_at(99)));
    }
    #[test]
    fn projected_fanout_does_not_duplicate_pending_outputs() {
        let tx = pending("fanout", "funding:0", 2048);
        let pending = HashMap::from([("fanout".into(), tx)]);
        let mut state = snapshot_at(10);
        state.utxos.insert(
            "funding:0".into(),
            AddressUtxo {
                txid: "funding".into(),
                output_index: 0,
                script: "".into(),
                satoshis: 1_000_000_000,
                height: 1,
            },
        );
        assert_eq!(projected_lanes(&state, &pending, 20_000), 2048);
        state.utxos.clear();
        for i in 0..2048 {
            state.utxos.insert(
                format!("fanout:{i}"),
                AddressUtxo {
                    txid: "fanout".into(),
                    output_index: i,
                    script: "".into(),
                    satoshis: 400_000,
                    height: 2,
                },
            );
        }
        assert_eq!(projected_lanes(&state, &pending, 20_000), 2048);
    }
    #[derive(Default)]
    struct MockState {
        height: u32,
        utxos: Vec<serde_json::Value>,
        submitted: Vec<String>,
        active: usize,
        max_active: usize,
    }

    async fn mock_rpc(
        state: Arc<std::sync::Mutex<MockState>>,
        journal: PathBuf,
    ) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let state = state.clone();
                let journal = journal.clone();
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let (header_end, length) = loop {
                        let mut buf = [0; 4096];
                        let n = stream.read(&mut buf).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        request.extend_from_slice(&buf[..n]);
                        if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                            let headers =
                                String::from_utf8_lossy(&request[..end]).to_ascii_lowercase();
                            let length: usize = headers
                                .lines()
                                .find_map(|line| line.strip_prefix("content-length:"))
                                .unwrap()
                                .trim()
                                .parse()
                                .unwrap();
                            break (end + 4, length);
                        }
                    };
                    while request.len() < header_end + length {
                        let mut buf = [0; 4096];
                        let n = stream.read(&mut buf).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        request.extend_from_slice(&buf[..n]);
                    }
                    let request: serde_json::Value =
                        serde_json::from_slice(&request[header_end..header_end + length]).unwrap();
                    let method = request["method"].as_str().unwrap();
                    if method == "sendrawtransaction" {
                        let raw = request["params"][0].as_str().unwrap().to_string();
                        // The server must observe a durable reservation before RPC starts.
                        assert!(std::fs::read_to_string(journal).unwrap().contains(&raw));
                        {
                            let mut state = state.lock().unwrap();
                            state.submitted.push(raw);
                            state.active += 1;
                            state.max_active = state.max_active.max(state.active);
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        state.lock().unwrap().active -= 1;
                        // Accept the bytes, then lose the response. The caller cannot
                        // distinguish this from a timeout before acceptance.
                        return;
                    }
                    let result = {
                        let state = state.lock().unwrap();
                        match method {
                            "getblockhash" => json!("mock-genesis"),
                            "getblockchaininfo" => {
                                json!({"blocks":state.height, "bestblockhash":format!("tip-{}",state.height)})
                            }
                            "getbestblockhash" => json!(format!("tip-{}", state.height)),
                            "getaddressutxos" => json!(state.utxos),
                            "getrawmempool" => json!([]),
                            "getrawtransaction" => json!({"vin":[{}], "confirmations":1}),
                            other => panic!("unexpected RPC {other}"),
                        }
                    };
                    let body = serde_json::to_string(
                        &json!({"result":result, "error":null, "id":request["id"]}),
                    )
                    .unwrap();
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\nContent-Type: application/json\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    stream.write_all(response.as_bytes()).await.unwrap();
                });
            }
        });
        (endpoint, task)
    }

    #[tokio::test]
    async fn ambiguous_submission_restart_reorg_and_expiry_keep_inputs_reserved() {
        use secp256k1::{PublicKey, Secp256k1, SecretKey};
        use zcash_transparent::address::TransparentAddress;
        let dir = scratch();
        let path = dir.join("journal.jsonl");
        let secret = SecretKey::from_slice(&[9; 32]).unwrap();
        let public = PublicKey::from_secret_key(&Secp256k1::new(), &secret);
        let recipient = TransparentAddress::from_pubkey(&public);
        let TransparentAddress::PublicKeyHash(hash) = recipient else {
            unreachable!()
        };
        let address = zebra_chain::transparent::Address::from_pub_key_hash(
            zebra_chain::parameters::NetworkKind::Testnet,
            hash,
        );
        let key_path = dir.join("key.json");
        std::fs::write(&key_path, serde_json::to_vec(&json!({"name":"test", "secret_key_hex":hex::encode(secret.secret_bytes()), "public_key_hex":hex::encode(public.serialize()), "address":address.to_string()})).unwrap()).unwrap();
        let script = hex::encode(address.script().as_raw_bytes());
        let funding = json!({"txid":"11".repeat(32), "outputIndex":0, "script":script, "satoshis":1_000_000, "height":1});
        let state = Arc::new(std::sync::Mutex::new(MockState {
            height: 10,
            utxos: vec![funding.clone()],
            ..Default::default()
        }));
        let (endpoint, server) = mock_rpc(state.clone(), path.clone()).await;
        let options = || Options {
            rpc_endpoint: endpoint.clone(),
            funded_key_path: Some(key_path.to_str().unwrap().into()),
            journal: path.clone(),
            rate: 100,
            lanes: 1,
            concurrency: 2,
            null_data_bytes: 80,
            max_pending_transactions: 0,
            refresh_secs: 0.1,
            duration_secs: 1,
            trace: None,
        };
        run(options()).await.unwrap();
        let first = state.lock().unwrap().submitted[0].clone();
        assert_eq!(state.lock().unwrap().submitted.len(), 1);
        run(options()).await.unwrap();
        assert_eq!(
            state.lock().unwrap().submitted,
            vec![first.clone(), first.clone()]
        );
        let (journal, pending) =
            Journal::open(&path, "mock-genesis".into(), address.to_string()).unwrap();
        drop(journal);
        let parent = pending.values().next().unwrap().clone();
        {
            let mut state = state.lock().unwrap();
            state.height = 11;
            state.utxos = vec![
                json!({"txid":parent.txid, "outputIndex":0, "script":script, "satoshis":980_000, "height":11}),
            ];
        }
        run(options()).await.unwrap();
        assert_eq!(state.lock().unwrap().submitted.len(), 3);
        assert_ne!(state.lock().unwrap().submitted[2], first);
        // Reorg removes the child UTXO and restores the original funding input.
        // The generator retries the original bytes rather than a conflicting spend.
        {
            let mut state = state.lock().unwrap();
            state.height = 10;
            state.utxos = vec![funding.clone()];
        }
        run(options()).await.unwrap();
        assert_eq!(state.lock().unwrap().submitted.len(), 4);
        assert_eq!(state.lock().unwrap().submitted[3], first);
        // Once the old transaction expires and is absent, a fresh spend can use it.
        state.lock().unwrap().height = 100;
        run(options()).await.unwrap();
        assert_eq!(state.lock().unwrap().submitted.len(), 5);
        assert_ne!(state.lock().unwrap().submitted[4], first);
        assert!(state.lock().unwrap().max_active <= 2);
        {
            let mut state = state.lock().unwrap();
            state.utxos = (1..=8).map(|i| json!({"txid":"22".repeat(32), "outputIndex":i, "script":script, "satoshis":1_000_000, "height":1})).collect();
        }
        run(options()).await.unwrap();
        assert_eq!(state.lock().unwrap().submitted.len(), 13);
        assert_eq!(state.lock().unwrap().max_active, 2);
        // Bootstrap must reserve fanouts before spending the source pool on
        // ordinary traffic. Two fanouts expand eight inputs to 32 lanes.
        {
            let mut state = state.lock().unwrap();
            state.utxos = (0..8).map(|i| json!({"txid":"33".repeat(32), "outputIndex":i, "script":script, "satoshis":1_000_000, "height":1})).collect();
        }
        let mut bootstrap_options = options();
        bootstrap_options.lanes = 32;
        bootstrap_options.duration_secs = 2;
        let bootstrap_trace = dir.join("bootstrap.jsonl");
        bootstrap_options.trace = Some(bootstrap_trace.clone());
        run(bootstrap_options).await.unwrap();
        let (journal, saved) =
            Journal::open(&path, "mock-genesis".into(), address.to_string()).unwrap();
        drop(journal);
        let fanouts: Vec<_> = saved
            .values()
            .filter(|tx| tx.input.starts_with(&"33".repeat(32)))
            .cloned()
            .collect();
        assert_eq!(fanouts.len(), 8);
        assert_eq!(fanouts.iter().map(|tx| tx.output_count).sum::<usize>(), 32);
        let events: Vec<serde_json::Value> = std::fs::read_to_string(&bootstrap_trace)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let built: Vec<_> = events
            .iter()
            .filter(|event| event["event"] == "built")
            .collect();
        assert_eq!(built[0]["outputs"], 22);
        assert_eq!(built[1]["outputs"], 4);
        assert!(built[2..].iter().all(|event| event["outputs"] == 1));
        // Once the fanouts confirm, all 32 outputs become self-spend lanes.
        {
            let mut state = state.lock().unwrap();
            state.height = 101;
            state.utxos = fanouts.iter().flat_map(|tx| {
                (0..tx.output_count).map(|i| json!({"txid":tx.txid, "outputIndex":i, "script":script, "satoshis":(1_000_000-tx.fee)/tx.output_count as u64, "height":101}))
            }).collect();
        }
        let before = state.lock().unwrap().submitted.len();
        let mut steady_options = options();
        steady_options.lanes = 32;
        steady_options.duration_secs = 2;
        run(steady_options).await.unwrap();
        assert_eq!(state.lock().unwrap().submitted.len() - before, 32);
        // An unattainable lane target reports a funding event instead of waiting
        // silently for a fanout that cannot be constructed.
        {
            let mut state = state.lock().unwrap();
            state.utxos = vec![
                json!({"txid":"44".repeat(32), "outputIndex":0, "script":script, "satoshis":50_000, "height":1}),
            ];
        }
        let mut poor_options = options();
        poor_options.lanes = 2048;
        let poor_trace = dir.join("poor.jsonl");
        poor_options.trace = Some(poor_trace.clone());
        run(poor_options).await.unwrap();
        let events = std::fs::read_to_string(poor_trace).unwrap();
        assert!(events.contains("lane_target_unreachable"));
        state.lock().unwrap().height = 10;
        assert!(
            run(options())
                .await
                .unwrap_err()
                .to_string()
                .contains("chain reorg crossed")
        );
        server.abort();
        std::fs::remove_dir_all(dir).unwrap();
    }
}
