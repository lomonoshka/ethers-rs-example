use ::futures::stream;
use ethers::contract::Multicall;
use ethers::providers::{Http, Middleware, Provider, StreamExt};
use ethers::types::ValueOrArray::Value;
use ethers::types::{Filter, H160, H256, U256};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;
use dotenv::dotenv;

include!(concat!(env!("OUT_DIR"), "/UniswapV2Pool.rs"));
include!(concat!(env!("OUT_DIR"), "/ERC20.rs"));

#[derive(Debug)]
struct PoolSingleTx {
    address: H160,
    sync_event: bool,
    swap_event: bool,
}

impl Default for PoolSingleTx {
    fn default() -> Self {
        Self {
            address: H160([0; 20]),
            swap_event: false,
            sync_event: false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct PoolInfo {
    pair_address: H160,
    token0_address: H160,
    token1_address: H160,
    token0_symbol: String,
    token1_symbol: String,
    token0_reserve_get_reserves: U256,
    token1_reserve_get_reserves: U256,
    token0_reserve_balance_of: U256,
    token1_reserve_balance_of: U256,
    block_num: u64,
    strange_reserves: bool,
}

impl Default for PoolInfo {
    fn default() -> Self {
        Self {
            pair_address: H160([0; 20]),
            token0_address: H160([0; 20]),
            token1_address: H160([0; 20]),
            token0_symbol: String::new(),
            token1_symbol: String::new(),
            token0_reserve_get_reserves: U256([0; 4]),
            token1_reserve_get_reserves: U256([0; 4]),
            token0_reserve_balance_of: U256([0; 4]),
            token1_reserve_balance_of: U256([0; 4]),
            block_num: 0,
            strange_reserves: false,
        }
    }
}

struct SubSegments {
    current: usize,
    end: usize,
    step: usize,
}

impl Iterator for SubSegments {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current > self.end {
            return None;
        }
        let current = self.current;
        self.current += self.step + 1;

        Some((current, min(self.current - 1, self.end)))
    }
}

fn sub_segments(start: usize, end: usize, step: usize) -> SubSegments {
    SubSegments {
        current: start,
        end,
        step,
    }
}

const GET_LOGS_BATCH_SIZE: usize = 500;
const RPC_LIMIT: usize = 10;
const MULTICALL_LIMIT: usize = 500;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let rpc_url = std::env::var("RPC_URL").expect("RPC_URL must be set.");
    let client = Arc::new(Provider::<Http>::try_from(rpc_url)?);
    let start_block: usize = 52_900_000;
    let end_block: usize = 53_000_000;

    let interface = UniswapV2Pool::new(H160([0; 20]), client.clone());
    let swap_filter = interface.swap_filter().filter;
    let sync_filter = interface.sync_filter().filter;

    let mut swap_filter_topic0 = H256([0; 32]);
    if let Value(Some(topic0)) = swap_filter.topics[0].clone().unwrap() {
        swap_filter_topic0 = topic0;
    }
    let mut sync_filter_topic0 = H256([0; 32]);
    if let Value(Some(topic0)) = sync_filter.topics[0].clone().unwrap() {
        sync_filter_topic0 = topic0;
    }


    let shared_addresses = Rc::new(RefCell::new(HashSet::<H160>::new()));
    let shared_single_tx_logs = Rc::new(RefCell::new(HashMap::<(H256, H160), PoolSingleTx>::new()));

    let segments = sub_segments(start_block, end_block, GET_LOGS_BATCH_SIZE).collect::<Vec<_>>();

    let futures = segments
        .iter()
        .map(|(from_block, to_block)| async {
            let filter = Filter::new()
                .topic0(vec![sync_filter_topic0, swap_filter_topic0])
                .from_block(*from_block)
                .to_block(*to_block);
            let logs = client.get_logs(&filter).await.unwrap();
            let mut addresses = shared_addresses.borrow_mut();
            let mut single_tx_logs = shared_single_tx_logs.borrow_mut();
            for log in logs {
                if addresses.contains(&log.address) {
                    continue;
                }
                let key = (log.transaction_hash.unwrap(), log.address);
                let tx = single_tx_logs.entry(key).or_insert(PoolSingleTx {
                    address: log.address,
                    ..Default::default()
                });

                if log.topics[0] == sync_filter_topic0 {
                    tx.sync_event = true;
                } else if log.topics[0] == swap_filter_topic0 {
                    tx.swap_event = true;
                }

                if tx.swap_event && tx.sync_event {
                    addresses.insert(tx.address);
                }
            }
        })
        .collect::<Vec<_>>();

    let stream = stream::iter(futures).buffer_unordered(RPC_LIMIT);

    let start = Instant::now();
    stream.collect::<Vec<_>>().await;
    let duration = start.elapsed();
    println!("Finished fetching logs for {} s", duration.as_secs_f64());

    let addresses = shared_addresses.borrow_mut();
    let addresses = addresses.iter().collect::<Vec<_>>();
    let shared_pool_candidates = Rc::new(RefCell::new(Vec::<PoolInfo>::new()));

    let futures: Vec<_> = addresses
        .chunks(MULTICALL_LIMIT / 2)
        .map(|addresses_chunk| async {
            let mut multicall = Multicall::new(client.clone(), None).await.unwrap();
            for address in addresses_chunk.iter() {
                let pool_candidate = UniswapV2Pool::new(**address, client.clone());
                multicall.add_call(pool_candidate.token_0(), true);
                multicall.add_call(pool_candidate.token_1(), true);
            }
            let results = multicall.call_array::<H160>().await.unwrap();

            let mut pool_candidates = shared_pool_candidates.borrow_mut();
            for (i, tokens) in results.chunks(2).enumerate() {
                pool_candidates.push(PoolInfo {
                    pair_address: *addresses_chunk[i],
                    token0_address: tokens[0],
                    token1_address: tokens[1],
                    ..Default::default()
                });
            }
        })
        .collect();

    let stream = stream::iter(futures).buffer_unordered(RPC_LIMIT);
    let start = Instant::now();
    stream.collect::<Vec<_>>().await;
    let duration = start.elapsed();
    println!(
        "Finished getting token addresses for {} s",
        duration.as_secs_f64()
    );

    let mut pool_candidates = shared_pool_candidates.borrow_mut();
    let futures = pool_candidates
        .chunks_mut(MULTICALL_LIMIT / 5)
        .map(|pool_candidates_chunk| async {
            let mut multicall = Multicall::new(client.clone(), None).await.unwrap();
            for pool_candidate in pool_candidates_chunk.iter() {
                let pool = UniswapV2Pool::new(pool_candidate.pair_address, client.clone());
                let token0 = ERC20::new(pool_candidate.token0_address, client.clone());
                let token1 = ERC20::new(pool_candidate.token1_address, client.clone());

                multicall.add_call(pool.get_reserves(), true);
                multicall.add_call(token0.balance_of(pool_candidate.pair_address), true);
                multicall.add_call(token1.balance_of(pool_candidate.pair_address), true);
                multicall.add_call(token0.symbol(), true);
                multicall.add_call(token1.symbol(), true);
            }
            multicall.add_get_block_number();
            let mut results = multicall.call_raw().await.unwrap();
            let block_number = results.pop().unwrap().clone().unwrap().into_uint().unwrap();

            for (i, single_pool_result) in results.chunks(5).enumerate() {
                let get_reserves_result =
                    single_pool_result[0].clone().unwrap().into_tuple().unwrap();
                let token0_reserve_get_reserves =
                    get_reserves_result[0].clone().into_uint().unwrap();
                let token1_reserve_get_reserves =
                    get_reserves_result[1].clone().into_uint().unwrap();

                let token0_reserve_balance_of =
                    single_pool_result[1].clone().unwrap().into_uint().unwrap();
                let token1_reserve_balance_of =
                    single_pool_result[2].clone().unwrap().into_uint().unwrap();
                let token0_symbol = single_pool_result[3]
                    .clone()
                    .unwrap()
                    .into_string()
                    .unwrap();
                let token1_symbol = single_pool_result[4]
                    .clone()
                    .unwrap()
                    .into_string()
                    .unwrap();

                let pool = &mut pool_candidates_chunk[i];
                pool.token0_reserve_get_reserves = token0_reserve_get_reserves;
                pool.token1_reserve_get_reserves = token1_reserve_get_reserves;
                pool.token0_reserve_balance_of = token0_reserve_balance_of;
                pool.token1_reserve_balance_of = token1_reserve_balance_of;
                pool.token0_symbol = token0_symbol;
                pool.token1_symbol = token1_symbol;
                pool.block_num = block_number.as_u64();
                pool.strange_reserves = token0_reserve_balance_of != token0_reserve_get_reserves
                    || token1_reserve_balance_of != token1_reserve_get_reserves;
            }
        })
        .collect::<Vec<_>>();

    let stream = stream::iter(futures).buffer_unordered(RPC_LIMIT);
    let start = Instant::now();
    stream.collect::<Vec<_>>().await;
    let duration = start.elapsed();
    println!(
        "Finished getting pool info for {} s",
        duration.as_secs_f64()
    );

    let result_file_path = Path::new("./src/result.json");
    if result_file_path.exists() {
        std::fs::remove_file(&result_file_path)?;
    }
    let output_file = File::create(result_file_path)?;
    let mut writer = BufWriter::new(output_file);
    serde_json::to_writer::<_, Vec<PoolInfo>>(&mut writer, pool_candidates.as_ref())?;
    writer.flush()?;

    Ok(())
}
