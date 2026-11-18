use anyhow::{Context, Result, ensure};
use zcash_primitives::transaction::{
    builder::{BuildConfig, Builder, BundlePadding},
    fees::zip317,
};
use zcash_protocol::{consensus::BlockHeight, value::Zatoshis};
use zcash_transparent::{
    address::{Script, TransparentAddress},
    builder::TransparentSigningSet,
    bundle::{OutPoint, TxOut},
};

use super::{
    TxblastNetworkParams,
    prover::{NoSaplingOutputProver, NoSaplingSpendProver},
    rpc::AddressUtxo,
    transparent::FundedKey,
};

pub(crate) const MAX_OUTPUTS: usize = 2048;
pub(crate) const MIN_OUTPUT_ZATS: u64 = 10_000;
pub(crate) const EXPIRY_BLOCKS: u32 = 40;

#[derive(Debug)]
pub(crate) struct Built {
    pub txid: String,
    pub raw_hex: String,
    pub expiry_height: u32,
    /// Number of spendable outputs; excludes OP_RETURN.
    pub output_count: usize,
    pub fee: u64,
    pub bytes: usize,
}

/// Compute the fee from the same output scripts used by the workload builder.
pub(crate) fn self_spend_fee(null_data_bytes: usize) -> Result<u64> {
    use zcash_primitives::transaction::fees::{
        FeeRule,
        transparent::{InputSize, OutputView},
    };
    let mut outputs = zcash_transparent::builder::TransparentBuilder::empty();
    outputs
        .add_output(&TransparentAddress::PublicKeyHash([0; 20]), Zatoshis::ZERO)
        .map_err(|e| anyhow::anyhow!("adding fee output: {e}"))?;
    if null_data_bytes != 0 {
        outputs
            .add_null_data_output(&vec![0; null_data_bytes])
            .map_err(|e| anyhow::anyhow!("adding fee OP_RETURN: {e}"))?;
    }
    let fee = zip317::FeeRule::standard()
        .fee_required(
            &TxblastNetworkParams::LocalGenesis,
            BlockHeight::from_u32(1),
            [InputSize::STANDARD_P2PKH],
            outputs.outputs().iter().map(OutputView::serialized_size),
            0,
            0,
            0,
            0,
        )
        .map_err(|e| anyhow::anyhow!("calculating workload fee: {e}"))?;
    Ok(u64::from(fee))
}

/// Spend one confirmed non-coinbase UTXO into owned P2PKH outputs.
/// The caller must verify confirmation and coinbase status before reserving it.
pub(crate) fn build(
    key: &FundedKey,
    input: &AddressUtxo,
    tip: u32,
    outputs: usize,
    null_data_bytes: usize,
) -> Result<Built> {
    ensure!(
        (1..=MAX_OUTPUTS).contains(&outputs),
        "output count must be 1..={MAX_OUTPUTS}"
    );
    ensure!(
        null_data_bytes <= 80,
        "OP_RETURN payload must be at most 80 bytes"
    );
    let target = tip.checked_add(1).context("target height overflow")?;
    let expiry = target
        .checked_add(EXPIRY_BLOCKS)
        .context("expiry height overflow")?;
    let expiry = match super::local_genesis_nu7_height() {
        Some(upgrade) if target < upgrade => expiry.min(upgrade - 1),
        _ => expiry,
    };
    let recipient = TransparentAddress::from_pubkey(&key.public_key);
    let script: Script = recipient.script().into();
    ensure!(
        hex::decode(&input.script).context("invalid input script hex")? == script.0.0,
        "input script does not belong to the funded key"
    );
    ensure!(
        key.address.script().as_raw_bytes() == script.0.0.as_slice(),
        "funded address does not match public key"
    );
    let mut txid: [u8; 32] = hex::decode(&input.txid)
        .context("invalid input txid")?
        .try_into()
        .map_err(|_| anyhow::anyhow!("input txid must contain 32 bytes"))?;
    txid.reverse();
    let outpoint = OutPoint::new(txid, input.output_index);
    let coin = TxOut::new(
        Zatoshis::from_u64(input.satoshis).context("invalid input value")?,
        script,
    );

    let make_builder = |values: &[u64]| -> Result<Builder<TxblastNetworkParams, ()>> {
        let mut builder = Builder::new(
            TxblastNetworkParams::LocalGenesis,
            BlockHeight::from_u32(target),
            BuildConfig::Standard {
                sapling_anchor: None,
                orchard_anchor: None,
                ironwood_anchor: None,
                orchard_padding: BundlePadding::DEFAULT,
                ironwood_padding: BundlePadding::DEFAULT,
            },
        )
        .with_expiry_height(BlockHeight::from_u32(expiry));
        builder
            .add_transparent_p2pkh_input(key.public_key, outpoint.clone(), coin.clone())
            .map_err(|error| anyhow::anyhow!("adding transparent input: {error}"))?;
        for value in values {
            builder
                .add_transparent_output(
                    &recipient,
                    Zatoshis::from_u64(*value).context("invalid output value")?,
                )
                .map_err(|error| anyhow::anyhow!("adding transparent output: {error}"))?;
        }
        if null_data_bytes != 0 {
            builder
                .add_transparent_null_data_output::<zip317::FeeError>(&vec![0; null_data_bytes])
                .map_err(|error| anyhow::anyhow!("adding OP_RETURN: {error}"))?;
        }
        Ok(builder)
    };
    let fee_rule = zip317::FeeRule::standard();
    let fee = make_builder(&vec![0; outputs])?
        .get_fee(&fee_rule)
        .map_err(|error| anyhow::anyhow!("calculating transparent fee: {error}"))?;
    let fee = u64::from(fee);
    let available = input
        .satoshis
        .checked_sub(fee)
        .context("input cannot pay transaction fee")?;
    let each = available / outputs as u64;
    ensure!(
        each >= MIN_OUTPUT_ZATS,
        "input cannot fund {outputs} outputs of at least {MIN_OUTPUT_ZATS} zats after fee {fee}"
    );
    let mut values = vec![each; outputs];
    values[0] += available % outputs as u64;
    let builder = make_builder(&values)?;
    let mut signing = TransparentSigningSet::new();
    signing.add_key(key.secret_key);
    let result = builder
        .build(
            &signing,
            &[],
            &[],
            rand_core_10::UnwrapErr(getrandom_04::SysRng),
            &NoSaplingSpendProver,
            &NoSaplingOutputProver,
            &fee_rule,
        )
        .map_err(|error| anyhow::anyhow!("building transparent transaction: {error}"))?;
    let tx = result.transaction();
    let mut bytes = Vec::new();
    tx.write(&mut bytes)
        .context("serializing transparent transaction")?;
    ensure!(
        bytes.len() <= 250_000,
        "transaction exceeds standard size limit"
    );
    Ok(Built {
        txid: tx.txid().to_string(),
        raw_hex: hex::encode(&bytes),
        expiry_height: expiry,
        output_count: outputs,
        fee,
        bytes: bytes.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use secp256k1::{PublicKey, Secp256k1, SecretKey};
    use zcash_primitives::transaction::Transaction;
    use zcash_protocol::consensus::BranchId;
    use zebra_chain::{parameters::NetworkKind, transparent::Address};

    fn fixture() -> (FundedKey, AddressUtxo) {
        let secret_key = SecretKey::from_slice(&[7; 32]).unwrap();
        let public_key = PublicKey::from_secret_key(&Secp256k1::new(), &secret_key);
        let recipient = TransparentAddress::from_pubkey(&public_key);
        let TransparentAddress::PublicKeyHash(hash) = recipient else {
            unreachable!()
        };
        let key = FundedKey {
            name: "test".into(),
            secret_key,
            public_key,
            address: Address::from_pub_key_hash(NetworkKind::Testnet, hash),
        };
        let script: Script = recipient.script().into();
        let input = AddressUtxo {
            txid: "0123456789abcdef".repeat(4),
            output_index: 3,
            script: hex::encode(script.0.0),
            satoshis: 100_000_000,
            height: 1,
        };
        (key, input)
    }

    fn decode(built: &Built) -> Transaction {
        Transaction::read(
            hex::decode(&built.raw_hex).unwrap().as_slice(),
            BranchId::for_height(
                &TxblastNetworkParams::LocalGenesis,
                BlockHeight::from_u32(101),
            ),
        )
        .unwrap()
    }

    #[test]
    fn self_spend_serializes_with_expected_input_and_no_shielded_bundles() {
        let (key, input) = fixture();
        let built = build(&key, &input, 100, 1, 80).unwrap();
        let tx = decode(&built);
        assert_eq!(tx.txid().to_string(), built.txid);
        assert_eq!(u32::from(tx.expiry_height()), 141);
        assert!(tx.sapling_bundle().is_none());
        assert!(tx.orchard_bundle().is_none());
        assert!(tx.ironwood_bundle().is_none());
        let bundle = tx.transparent_bundle().unwrap();
        assert_eq!(bundle.vin.len(), 1);
        assert_eq!(bundle.vout.len(), 2);
        assert_eq!(bundle.vin[0].prevout().n(), input.output_index);
        let mut expected_txid = hex::decode(&input.txid).unwrap();
        expected_txid.reverse();
        assert_eq!(bundle.vin[0].prevout().hash(), expected_txid.as_slice());
        assert_eq!(u64::from(bundle.vout[1].value()), 0);
        assert_eq!(
            u64::from(bundle.vout[0].value()) + built.fee,
            input.satoshis
        );
        assert_eq!(built.fee, 20_000);
        assert_eq!(built.output_count, 1);
        assert!(built.bytes > 250 && built.bytes < 400);
        assert!(4_194_304 / built.bytes < 20_000);
        println!(
            "self_spend bytes={} fee_zats={} sigops=1 payload_bytes=80",
            built.bytes, built.fee
        );
    }

    #[test]
    fn fanout_preserves_value_and_stays_below_policy_limits() {
        use zakura_script::Sigops;
        use zebra_chain::serialization::ZcashDeserialize;
        let (key, input) = fixture();
        let built = build(&key, &input, 100, MAX_OUTPUTS, 0).unwrap();
        let tx = decode(&built);
        let bundle = tx.transparent_bundle().unwrap();
        let output_total: u64 = bundle
            .vout
            .iter()
            .map(|output| u64::from(output.value()))
            .sum();
        assert_eq!(output_total + built.fee, input.satoshis);
        assert_eq!(built.fee, MAX_OUTPUTS as u64 * 5000);
        assert_eq!(bundle.vout.len(), MAX_OUTPUTS);
        println!(
            "fanout bytes={} fee_zats={} sigops={} outputs={MAX_OUTPUTS}",
            built.bytes, built.fee, MAX_OUTPUTS
        );
        assert!(
            bundle
                .vout
                .iter()
                .all(|output| u64::from(output.value()) >= MIN_OUTPUT_ZATS)
        );
        assert!(built.bytes < 250_000);
        let bytes = hex::decode(&built.raw_hex).unwrap();
        let daemon_tx =
            zebra_chain::transaction::Transaction::zcash_deserialize(bytes.as_slice()).unwrap();
        assert_eq!(daemon_tx.sigops().unwrap(), MAX_OUTPUTS as u32);
        assert!(daemon_tx.sigops().unwrap() < 4000);
    }

    #[test]
    fn signature_verifies_against_daemon_sighash() {
        use zakura_script::Sigops;
        use zebra_chain::serialization::ZcashDeserialize;
        use zebra_chain::transaction::HashType;
        let (key, input) = fixture();
        let built = build(&key, &input, 100, 1, 80).unwrap();
        let bytes = hex::decode(&built.raw_hex).unwrap();
        let daemon_tx =
            zebra_chain::transaction::Transaction::zcash_deserialize(bytes.as_slice()).unwrap();
        assert_eq!(daemon_tx.sigops().unwrap(), 1);
        assert_eq!(daemon_tx.hash().to_string(), built.txid);
        let previous_output = zebra_chain::transparent::Output::new(
            input.satoshis.try_into().unwrap(),
            key.address.script(),
        );
        let hasher = daemon_tx
            .sighasher(
                daemon_tx.network_upgrade().unwrap(),
                std::sync::Arc::new(vec![previous_output]),
            )
            .unwrap();
        let hash: [u8; 32] = hasher.sighash(HashType::ALL, Some((0, vec![]))).into();
        let tx = decode(&built);
        let script = &tx.transparent_bundle().unwrap().vin[0].script_sig().0.0;
        let signature_len = script[0] as usize;
        assert_eq!(script[signature_len], 1);
        let signature = secp256k1::ecdsa::Signature::from_der(&script[1..signature_len]).unwrap();
        Secp256k1::new()
            .verify_ecdsa(
                &secp256k1::Message::from_digest(hash),
                &signature,
                &key.public_key,
            )
            .unwrap();
        let mut wrong_hash = hash;
        wrong_hash[0] ^= 1;
        assert!(
            Secp256k1::new()
                .verify_ecdsa(
                    &secp256k1::Message::from_digest(wrong_hash),
                    &signature,
                    &key.public_key
                )
                .is_err()
        );
    }

    #[test]
    fn rejects_invalid_ownership_and_unfunded_outputs() {
        let (key, mut input) = fixture();
        for (outputs, payload) in [(0, 0), (2049, 0), (1, 81)] {
            assert!(build(&key, &input, 100, outputs, payload).is_err());
        }
        input.satoshis = 19_999;
        assert!(build(&key, &input, 100, 1, 0).is_err());
        input.satoshis = 100_000_000;
        input.script = "51".into();
        assert!(build(&key, &input, 100, 1, 0).is_err());
    }

    #[test]
    #[ignore = "manual signing throughput measurement"]
    fn signing_throughput() {
        let (key, input) = fixture();
        let start = std::time::Instant::now();
        let mut bytes = 0;
        for _ in 0..10_000 {
            bytes += build(&key, &input, 100, 1, 80).unwrap().bytes;
        }
        let elapsed = start.elapsed().as_secs_f64();
        println!(
            "signed=10000 elapsed_seconds={elapsed:.3} transactions_per_second={:.1} bytes_per_second={:.1}",
            10_000.0 / elapsed,
            bytes as f64 / elapsed
        );
    }
    #[test]
    fn workload_payload_profiles() {
        let (key, input) = fixture();
        for payload in [0, 1, 22, 23, 24, 25, 80] {
            let built = build(&key, &input, 100, 1, payload).unwrap();
            assert_eq!(built.fee, self_spend_fee(payload).unwrap());
            println!(
                "profile payload_bytes={payload} tx_bytes={} fee_zats={}",
                built.bytes, built.fee
            );
        }
    }
}
