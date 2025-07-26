// aggregator_manager.rs
/// 多合约管理器

use std::collections::{HashMap, HashSet};
use chrono::NaiveTime;

use crate::md_structures::{TickData, BarData};
use crate::bar1min_aggregator::{Bar1MinAggregator};
use crate::bar5min_aggregator::{Bar5MinAggregator};
use crate::trade_session_loader::TradeSessionMap;       // 来自 trade_session 模块
use crate::common_utils::*;                      // 你的工具函数模块
use rayon::prelude::*;

#[derive(Default, Debug)]
pub struct AggregatorManager {
    bar1min_aggregators: HashMap<String, Bar1MinAggregator>,
    bar5min_aggregators: HashMap<String, Bar5MinAggregator>,
    active_contracts: HashSet<String>,
}

impl AggregatorManager {
    pub fn new() -> Self {
        Self::default()
    }

    fn extract_comd(contract: &str) -> String {
        contract.chars().take_while(|c| c.is_ascii_alphabetic()).collect()
    }

    pub fn update_active_contracts(
        &mut self,
        trade_sessions: &TradeSessionMap,
        now: NaiveTime,
    ) {
        self.active_contracts.clear();
        for (contract, _) in self.bar1min_aggregators.iter() {
            let comd = Self::extract_comd(contract);
            if trade_sessions.is_in_trading_session(&comd, now) {
                self.active_contracts.insert(contract.clone());
            }
        }
    }

    pub fn on_tick(&mut self, tick: &TickData) -> Option<BarData> {
        if !Bar1MinAggregator::validate_tick(tick) {
            return None;
        }

        let contract = to_string_field(&tick.contract);
        let aggr = self.bar1min_aggregators
            .entry(contract.clone())
            .or_insert_with(Bar1MinAggregator::new);
        aggr.on_tick(tick)
    }

    // pub fn flush_all_bar1min(&mut self) -> Vec<BarData> {
    //     self.bar1min_aggregators
    //         .values_mut()
    //         .filter_map(|aggr| aggr.flush())
    //         .collect()
    // }

    pub fn flush_all_bar1min_active(&mut self) -> Vec<BarData> {
        self.bar1min_aggregators
            // .iter_mut() // ← 串行遍历
            .par_iter_mut() // ← 并行遍历
            .filter_map(|(contract, aggr)| {
                if self.active_contracts.contains(contract) {
                    aggr.flush()
                } else {
                    None
                }
            })
            .collect()
    }

    /// 把 1min bar 喂入 5min 聚合器
    pub fn on_bar_1min(&mut self, bar: &BarData) -> Option<BarData> {
        let aggr5 = self
            .bar5min_aggregators        
            .entry(bar.contract.clone())
            .or_insert_with(Bar5MinAggregator::new);

        aggr5.on_bar_1min(bar.clone())   // 需要拥有所有权，所以 clone
    }

    /// flush 产生活跃的 5min bar（内部先 flush 1min）
    pub fn flush_all_bar5min_active(&mut self) -> Vec<BarData> {
        let bars_1min = self.flush_all_bar1min_active();
        let mut bars_5min = Vec::new();

        for bar in bars_1min {
            let aggr5 = self
                .bar5min_aggregators
                .entry(bar.contract.clone())
                .or_insert_with(Bar5MinAggregator::new);

            if let Some(b5) = aggr5.on_bar_1min(bar) {
                bars_5min.push(b5);
            }
        }
        bars_5min
    }
}
