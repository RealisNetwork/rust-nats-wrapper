use log::info;
use colored::Colorize;

use std::fmt::Debug;

pub struct Logger;

impl Logger {
    pub fn got_message(topic: &str, request: impl Debug) {
        info!(
            "By topic - [{:^30}] - got message  - {:?}",
            topic.purple(),
            request
        )
    }

    pub fn sent_message(topic: &str, request: impl Debug) {
        info!(
            "By topic - [{:^30}] - sent message - {:?}",
            topic.magenta(),
            request
        )
    }
}