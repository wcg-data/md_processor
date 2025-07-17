// src/aggregator_manager.rs
/// 多合约管理器

use std::collections::{HashMap, HashSet};
use chrono::NaiveTime;

use crate::md_structures::{TickData, Bar1Min};
use crate::bar_aggregator::{BarAggregator};   // 来自 bar_aggregator.rs
use crate::trade_session_loader::TradeSessionMap;       // 来自 trade_session 模块
use crate::common_utils::*;                      // 你的工具函数模块

#[derive(Default)]
pub struct AggregatorManager {
    aggregators: HashMap<String, BarAggregator>,
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
        for (contract, _) in self.aggregators.iter() {
            let comd = Self::extract_comd(contract);
            if trade_sessions.is_in_trading_session(&comd, now) {
                self.active_contracts.insert(contract.clone());
            }
        }
    }

    pub fn on_tick(&mut self, tick: &TickData) -> Option<Bar1Min> {
        if !BarAggregator::validate_tick(tick) {
            return None;
        }

        let contract = to_string_field(&tick.contract);
        let aggr = self.aggregators
            .entry(contract.clone())
            .or_insert_with(BarAggregator::new);
        aggr.on_tick(tick)
    }

    pub fn flush_all(&mut self) -> Vec<Bar1Min> {
        self.aggregators
            .values_mut()
            .filter_map(|aggr| aggr.flush())
            .collect()
    }

    pub fn flush_all_active(&mut self) -> Vec<Bar1Min> {
        self.aggregators
            .iter_mut()
            .filter_map(|(contract, aggr)| {
                if self.active_contracts.contains(contract) {
                    aggr.flush()
                } else {
                    None
                }
            })
            .collect()
    }
}
