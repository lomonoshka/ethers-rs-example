use ethers::abi::RawLog;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::thread::sleep;
use std::{future, thread, time};
// use uniswap_v2_pool::SwapFilter;
use crate::uniswap_v2_pool::{SwapFilter, UniswapV2PoolEvents};
use ethers::contract::{decode_logs, EthEvent};
use ethers::prelude::EthLogDecode;
use ethers::providers::{
    Http, LogQueryError, Middleware, Provider, ProviderError, ProviderExt, StreamExt,
};
use ethers::types::ValueOrArray::Value;
use ethers::types::{Filter, Log, Topic, ValueOrArray, H160, H256, U64};
mod uniswap_v2_pool;

#[derive(Debug)]
struct SwapTx {
    address: H160,
    sync_event: bool,
    swap_event: bool,
}

impl Default for SwapTx {
    fn default() -> Self {
        Self {
            address: H160([0; 20]),
            swap_event: false,
            sync_event: false,
        }
    }
}

// 3. Add annotation
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rpc_url = "https://polygon.gateway.tenderly.co/WBWReuDdbSaypFTw9vO7t";
    let provider = Arc::new(Provider::<Http>::connect(rpc_url).await);
    let swap_filter = uniswap_v2_pool::SwapFilter::new(Filter::new(), &provider);
    let sync_filter = uniswap_v2_pool::SyncFilter::new(Filter::new(), &provider);

    let mut swap_filter_topic0 = H256([0; 32]);
    if let Value(Some(topic0)) = swap_filter.filter.topics[0].clone().unwrap() {
        swap_filter_topic0 = topic0;
    }
    let mut sync_filter_topic0 = H256([0; 32]);
    if let Value(Some(topic0)) = sync_filter.filter.topics[0].clone().unwrap() {
        sync_filter_topic0 = topic0;
    }

    let start_block: usize = 52_900_000;
    let end_block: usize = 52_900_100;
    // let end_block = 53_000_000;
    let batch_size = 1;

    let filter = Filter::new()
        .topic0(vec![sync_filter_topic0, swap_filter_topic0])
        .select(start_block..end_block);

    let mut stream = provider.get_logs_paginated(&filter, batch_size);

    let shared_pools = Rc::new(RefCell::new(HashSet::<H160>::new()));
    let shared_single_tx_logs = Rc::new(RefCell::new(HashMap::<(H256, H160), SwapTx>::new()));
    let mut pools = shared_pools.borrow_mut();
    let mut single_tx_logs = shared_single_tx_logs.borrow_mut();

    while let Some(Ok(log)) = stream.next().await {
        let block_number = log.block_number.unwrap();
        println!("{}", block_number);
        if block_number >= end_block.into() {
            break;
        };
        if pools.contains(&log.address) {
            continue;
        }
        let key = (log.transaction_hash.unwrap(), log.address);
        let tx = single_tx_logs.entry(key).or_insert(SwapTx {
            address: log.address,
            ..Default::default()
        });

        let raw_log = RawLog {
            topics: log.topics.clone(),
            data: log.data.to_vec(),
        };

        match uniswap_v2_pool::uniswap_v2_pool::UniswapV2PoolEvents::decode_log(&raw_log) {
            Ok(UniswapV2PoolEvents::SwapFilter(sf)) => {
                tx.swap_event = true;
                // println!("Got swap event");
            }
            Ok(UniswapV2PoolEvents::SyncFilter(sf)) => {
                tx.sync_event = true;
                // println!("Got sync event");
            }
            _ => {
                println!("Strange log")
            }
        }
        if tx.swap_event && tx.sync_event {
            pools.insert(tx.address);
            println!("Pools length {}", pools.len());
            println!("Current processed log block number {:?}", log.block_number);
        }
    }
    // println!("{:?}", logs.len());
    // stream.
    // stream.
    // stream
    //     .for_each(|log| async {
    //         println!("here");
    //         let mut pools = shared_pools.borrow_mut();
    //         let mut single_tx_logs = shared_single_tx_logs.borrow_mut();
    //         let log = log.unwrap();
    //         if pools.contains(&log.address) {
    //             return;
    //         }
    //         let key = (log.transaction_hash.unwrap(), log.address);
    //         let tx = single_tx_logs.entry(key).or_insert(SwapTx {
    //             address: log.address,
    //             ..Default::default()
    //         });

    //         let raw_log = RawLog {
    //             topics: log.topics.clone(),
    //             data: log.data.to_vec(),
    //         };

    //         match uniswap_v2_pool::uniswap_v2_pool::UniswapV2PoolEvents::decode_log(&raw_log) {
    //             Ok(UniswapV2PoolEvents::SwapFilter(sf)) => {
    //                 tx.swap_event = true;
    //                 // println!("Got swap event");
    //             }
    //             Ok(UniswapV2PoolEvents::SyncFilter(sf)) => {
    //                 tx.sync_event = true;
    //                 // println!("Got sync event");
    //             }
    //             _ => {
    //                 println!("Strange log")
    //             }
    //         }
    //         if tx.swap_event && tx.sync_event {
    //             pools.insert(tx.address);
    //             println!("Pools length {}", pools.len());
    //             println!("Current processed log block number {:?}", log.block_number);
    //         }
    //     })
    //     .await;

    // while true {
    //     sleep(time::Duration::from_millis(1000));
    //     println!();
    //             // println!("Current processed log block number {:?}", stream.size_hint());
    // }

    Ok(())
}
