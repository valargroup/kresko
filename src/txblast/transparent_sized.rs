//! Grows a signed 1-in-1-out lane spend to a target size with one OP_RETURN output.
//!
//! The Common builder caps OP_RETURN data at 80 bytes, zcashd's relay default. Zakura
//! nodes set the cap with `mempool.max_datacarrier_bytes`, so a private chain can admit
//! larger data outputs. [`resize`] appends the OP_RETURN output to a spend the builder
//! signed, lowers the lane output by the larger fee, and signs the input again against
//! the node's own sighash.

use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use secp256k1::{Message, Secp256k1};
use sha2::{Digest, Sha256};
use zebra_chain::amount::{Amount, NonNegative};
use zebra_chain::serialization::{ZcashDeserialize, ZcashSerialize};
use zebra_chain::transaction::{HashType, Transaction, zip317};
use zebra_chain::transparent::{Input, Output, Script};

use super::transparent::FundedKey;

/// Largest target size. A 2,000,000-byte block also holds a header and a coinbase.
pub(crate) const MAX_TX_BYTES: usize = 1_990_000;
/// Largest push the script interpreter accepts.
const MAX_PUSH_BYTES: usize = 520;
const OP_RETURN: u8 = 0x6a;
const OP_PUSHDATA1: u8 = 0x4c;
const OP_PUSHDATA2: u8 = 0x4d;
const SIGHASH_ALL: u8 = 0x01;

/// A resized spend.
pub(crate) struct Resized {
    pub txid: String,
    pub bytes: Vec<u8>,
    /// The fee the resized spend pays, in zatoshis.
    pub fee: u64,
}

/// Rewrites `base`, a signed spend of one P2PKH output worth `spent_value` locked by
/// `spent_script`, to about `target_bytes` bytes.
///
/// The spend's single output keeps its address and pays `spent_value` minus
/// `marginal_fee` per ZIP 317 logical action. The result can differ from
/// `target_bytes` by a byte or two because the new signature's length varies.
pub(crate) fn resize(
    base: &[u8],
    funded_key: &FundedKey,
    spent_value: u64,
    spent_script: &[u8],
    target_bytes: usize,
    marginal_fee: u64,
) -> Result<Resized> {
    ensure!(
        target_bytes <= MAX_TX_BYTES,
        "transaction size {target_bytes} exceeds {MAX_TX_BYTES} bytes"
    );
    let mut tx = Transaction::zcash_deserialize(base).context("decoding the base spend")?;
    ensure!(
        tx.inputs().len() == 1 && tx.outputs().len() == 1,
        "the base spend must have one input and one output"
    );
    let room = target_bytes.saturating_sub(base.len());
    let payload_bytes = payload_bytes_for(room).with_context(|| {
        format!(
            "transaction size {target_bytes} is below the smallest OP_RETURN spend, {} bytes",
            base.len() + output_bytes(script_bytes(0))
        )
    })?;
    let seed = tx.inputs()[0]
        .outpoint()
        .context("the base spend has a coinbase input")?;
    let payload = filler(&seed.hash.0, seed.index, payload_bytes);
    transparent_parts(&mut tx).1.push(Output::new(
        Amount::zero(),
        Script::new(&null_data_script(&payload)),
    ));

    let fee = marginal_fee * u64::from(zip317::conventional_actions(&tx));
    let change = spent_value
        .checked_sub(fee)
        .with_context(|| format!("lane value {spent_value} cannot pay the {fee}-zat fee"))?;
    transparent_parts(&mut tx).1[0].value = Amount::<NonNegative>::try_from(change)?;

    let nu = tx
        .network_upgrade()
        .context("the base spend names no network upgrade")?;
    let spent = Output::new(
        Amount::<NonNegative>::try_from(spent_value)?,
        Script::new(spent_script),
    );
    let sighash: [u8; 32] = tx
        .sighash(
            nu,
            HashType::ALL,
            Arc::new(vec![spent]),
            Some((0, spent_script.to_vec())),
        )?
        .into();
    let signature = Secp256k1::signing_only()
        .sign_ecdsa(&Message::from_digest(sighash), &funded_key.secret_key)
        .serialize_der();
    let public_key = funded_key.public_key.serialize();
    let mut unlock = Vec::with_capacity(3 + signature.len() + public_key.len());
    unlock.push((signature.len() + 1) as u8);
    unlock.extend_from_slice(&signature);
    unlock.push(SIGHASH_ALL);
    unlock.push(public_key.len() as u8);
    unlock.extend_from_slice(&public_key);
    match &mut transparent_parts(&mut tx).0[0] {
        Input::PrevOut { unlock_script, .. } => *unlock_script = Script::new(&unlock),
        Input::Coinbase { .. } => unreachable!("outpoint() returned Some"),
    }
    ensure!(
        zip317::conventional_actions(&tx) as u64 * marginal_fee == fee,
        "the new signature changed the spend's fee"
    );

    Ok(Resized {
        txid: tx.hash().to_string(),
        bytes: tx.zcash_serialize_to_vec()?,
        fee,
    })
}

/// Returns the transparent inputs and outputs of `tx` for editing.
fn transparent_parts(tx: &mut Transaction) -> (&mut Vec<Input>, &mut Vec<Output>) {
    match tx {
        Transaction::V1 {
            inputs, outputs, ..
        }
        | Transaction::V2 {
            inputs, outputs, ..
        }
        | Transaction::V3 {
            inputs, outputs, ..
        }
        | Transaction::V4 {
            inputs, outputs, ..
        }
        | Transaction::V5 {
            inputs, outputs, ..
        }
        | Transaction::V6 {
            inputs, outputs, ..
        } => (inputs, outputs),
    }
}

/// Returns the largest payload whose OP_RETURN output fits in `room` bytes, or `None`
/// if even an empty OP_RETURN output doesn't fit.
fn payload_bytes_for(room: usize) -> Option<usize> {
    // Push overhead is at most 3 bytes per 520, so this guess is never too small.
    let mut payload = room;
    loop {
        if output_bytes(script_bytes(payload)) <= room {
            return Some(payload);
        }
        payload = payload.checked_sub(1)?;
    }
}

/// Serialized size of an output whose script is `script` bytes long.
fn output_bytes(script: usize) -> usize {
    let length_prefix = match script {
        0..=0xfc => 1,
        0xfd..=0xffff => 3,
        _ => 5,
    };
    8 + length_prefix + script
}

/// Length of the OP_RETURN script [`null_data_script`] builds for `payload` bytes.
fn script_bytes(payload: usize) -> usize {
    let full = payload / MAX_PUSH_BYTES;
    let rest = payload % MAX_PUSH_BYTES;
    1 + full * (3 + MAX_PUSH_BYTES)
        + if rest == 0 {
            0
        } else {
            push_overhead(rest) + rest
        }
}

fn push_overhead(len: usize) -> usize {
    match len {
        1..=75 => 1,
        76..=255 => 2,
        _ => 3,
    }
}

/// OP_RETURN followed by `payload` in minimal pushes of at most [`MAX_PUSH_BYTES`].
///
/// [`filler`] never ends in a lone byte that has its own opcode, so every chunk uses a
/// length-prefixed push and [`script_bytes`] stays exact.
fn null_data_script(payload: &[u8]) -> Vec<u8> {
    let mut script = Vec::with_capacity(script_bytes(payload.len()));
    script.push(OP_RETURN);
    for chunk in payload.chunks(MAX_PUSH_BYTES) {
        match chunk.len() {
            len @ 1..=75 => script.push(len as u8),
            len @ 76..=255 => script.extend_from_slice(&[OP_PUSHDATA1, len as u8]),
            len => {
                script.push(OP_PUSHDATA2);
                script.extend_from_slice(&(len as u16).to_le_bytes());
            }
        }
        script.extend_from_slice(chunk);
    }
    script
}

/// Incompressible payload derived from the spent outpoint.
fn filler(txid: &[u8; 32], index: u32, len: usize) -> Vec<u8> {
    let mut payload = Vec::with_capacity(len);
    let mut counter = 0u64;
    while payload.len() < len {
        let block = Sha256::new()
            .chain_update(txid)
            .chain_update(index.to_le_bytes())
            .chain_update(counter.to_le_bytes())
            .finalize();
        let take = (len - payload.len()).min(block.len());
        payload.extend_from_slice(&block[..take]);
        counter += 1;
    }
    // A lone final byte of 0, 1–16 or 0x81 has its own opcode, which a minimal push
    // would use instead of a length prefix. 0x42 has none.
    if len % MAX_PUSH_BYTES == 1 {
        payload[len - 1] = 0x42;
    }
    payload
}

#[cfg(test)]
pub(crate) mod tests {
    use secp256k1::{PublicKey, SecretKey};
    use zcash_transparent::address::TransparentAddress;
    use zebra_chain::parameters::NetworkKind;
    use zebra_chain::transparent::Address;

    use super::*;
    use crate::txblast::TxblastNetworkParams;
    use crate::txblast::orchard::{TransparentInput, build_transparent_tx, transparent_fee};

    const SPENT_VALUE: u64 = 1_000_000_000;

    pub(crate) fn key() -> (FundedKey, TransparentAddress, Vec<u8>) {
        let secret_key = SecretKey::from_slice(&[7; 32]).unwrap();
        let public_key = PublicKey::from_secret_key(&Secp256k1::new(), &secret_key);
        let address = TransparentAddress::from_pubkey(&public_key);
        let TransparentAddress::PublicKeyHash(hash) = address else {
            unreachable!()
        };
        let key = FundedKey {
            name: "test".into(),
            secret_key,
            public_key,
            address: Address::from_pub_key_hash(NetworkKind::Testnet, hash),
        };
        let script = zcash_transparent::address::Script::from(address.script())
            .0
            .0;
        (key, address, script)
    }

    fn base(key: &FundedKey, address: TransparentAddress, script: &[u8]) -> Vec<u8> {
        build_transparent_tx(
            TxblastNetworkParams::LocalGenesis,
            key,
            &[TransparentInput {
                txid: &"0123456789abcdef".repeat(4),
                output_index: 3,
                script: &hex::encode(script),
                value: SPENT_VALUE,
            }],
            &[(address, SPENT_VALUE - transparent_fee(1, 1))],
            100,
        )
        .unwrap()
        .1
    }

    /// Checks `bytes` the way a node would: the input script verifies, the fee is
    /// paid, and the OP_RETURN script is all pushes of at most 520 bytes.
    fn check(bytes: &[u8], script: &[u8], fee: u64, payload: usize) {
        let tx = Arc::new(Transaction::zcash_deserialize(bytes).unwrap());
        let spent = Output::new(SPENT_VALUE.try_into().unwrap(), Script::new(script));
        zakura_script::CachedFfiTransaction::new(
            tx.clone(),
            Arc::new(vec![spent]),
            tx.network_upgrade().unwrap(),
        )
        .unwrap()
        .is_valid(0)
        .unwrap();
        assert_eq!(fee, 5_000 * u64::from(zip317::conventional_actions(&tx)));
        assert_eq!(u64::from(tx.outputs()[0].value), SPENT_VALUE - fee);

        let wire = tx.outputs()[1].lock_script.as_raw_bytes();
        assert_eq!(wire[0], OP_RETURN);
        let (mut cursor, mut pushed) = (1, 0);
        while cursor < wire.len() {
            let (len, prefix) = match wire[cursor] {
                len @ 1..=75 => (usize::from(len), 1),
                OP_PUSHDATA1 => (usize::from(wire[cursor + 1]), 2),
                OP_PUSHDATA2 => (
                    usize::from(u16::from_le_bytes([wire[cursor + 1], wire[cursor + 2]])),
                    3,
                ),
                op => panic!("non-push opcode {op:#x}"),
            };
            // Minimal pushes only: each length uses the shortest prefix.
            assert_eq!(prefix, push_overhead(len), "push of {len} bytes");
            assert!(len <= MAX_PUSH_BYTES);
            cursor += prefix + len;
            pushed += len;
        }
        assert_eq!(cursor, wire.len());
        assert_eq!(pushed, payload);
    }

    #[test]
    fn resized_spends_hit_their_target_and_verify() {
        let (key, address, script) = key();
        let base = base(&key, address, &script);
        for target in [
            base.len() + 10,
            250,
            300,
            1_000,
            100_000,
            1_000_000,
            MAX_TX_BYTES,
        ] {
            let resized = resize(&base, &key, SPENT_VALUE, &script, target, 5_000).unwrap();
            assert!(
                resized.bytes.len().abs_diff(target) <= 2,
                "target {target} built {}",
                resized.bytes.len()
            );
            let tx = Transaction::zcash_deserialize(resized.bytes.as_slice()).unwrap();
            assert_eq!(tx.hash().to_string(), resized.txid);
            let payload = payload_bytes_for(target - base.len()).unwrap();
            check(&resized.bytes, &script, resized.fee, payload);
        }
    }

    #[test]
    fn script_length_matches_the_built_script_at_push_boundaries() {
        for payload in [
            0, 1, 75, 76, 255, 256, 519, 520, 521, 1_040, 1_041, 1_000_000,
        ] {
            let built = null_data_script(&filler(&[1; 32], 0, payload));
            assert_eq!(built.len(), script_bytes(payload), "payload {payload}");
        }
    }

    #[test]
    fn rejects_targets_without_room_or_above_the_cap() {
        let (key, address, script) = key();
        let base = base(&key, address, &script);
        assert!(resize(&base, &key, SPENT_VALUE, &script, base.len() + 9, 5_000).is_err());
        assert!(resize(&base, &key, SPENT_VALUE, &script, MAX_TX_BYTES + 1, 5_000).is_err());
        assert!(resize(&base, &key, 20_000, &script, 100_000, 5_000).is_err());
    }
}
