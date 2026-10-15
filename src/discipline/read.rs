// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::{Discipline, ReadPolicy, Settings};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadDecision {
    Pending,
    Data(usize),
    EndOfInput,
    WouldBlock,
    TimedOut,
}

/// One admitted read's policy and timing snapshot, independent of IPC ids.
pub struct ReadRequest {
    pub capacity: usize,
    canonical: bool,
    policy: ReadPolicy,
    admitted_ns: u64,
    deadline_ns: u64,
    first_seen_ns: Option<u64>,
    generation: u64,
    last_byte_ns: u64,
}

impl ReadRequest {
    pub fn new(settings: Settings, capacity: usize, now_ns: u64, deadline_ns: u64) -> Self {
        Self {
            capacity,
            canonical: settings.canonical,
            policy: settings.read_policy,
            admitted_ns: now_ns,
            deadline_ns,
            first_seen_ns: None,
            generation: 0,
            last_byte_ns: now_ns,
        }
    }

    pub fn poll(&mut self, terminal: &Discipline, now_ns: u64, buffer: &mut [u8]) -> ReadDecision {
        let capacity = self.capacity.min(buffer.len());
        if capacity == 0 {
            return ReadDecision::Data(0);
        }
        let record = terminal.records.front();
        if record.is_some_and(|record| record.remaining == 0) {
            return ReadDecision::EndOfInput;
        }
        let available = record.map_or(0, |r| r.remaining).min(capacity);
        let decision = self.decide(
            available,
            capacity,
            record.is_some_and(|record| !record.open),
            terminal.last_input_ns,
            terminal.input_generation,
            now_ns,
        );
        if let ReadDecision::Data(count) = decision {
            for (dst, src) in buffer.iter_mut().zip(terminal.input.iter()).take(count) {
                *dst = *src;
            }
        }
        decision
    }

    /// Polls untouched binary input without transferring it into a cooked
    /// queue. The caller consumes exactly `Data(n)` only after reply commit.
    pub fn poll_raw(
        &mut self,
        available: usize,
        last_input_ns: u64,
        generation: u64,
        now_ns: u64,
    ) -> ReadDecision {
        self.decide(
            available.min(self.capacity),
            self.capacity,
            false,
            last_input_ns,
            generation,
            now_ns,
        )
    }

    #[must_use]
    pub fn available_policy(&self) -> bool {
        !self.canonical && self.policy == ReadPolicy::Available
    }

    fn decide(
        &mut self,
        available: usize,
        capacity: usize,
        sealed: bool,
        last_input_ns: u64,
        generation: u64,
        now_ns: u64,
    ) -> ReadDecision {
        if available > 0 {
            if self.first_seen_ns.is_none() {
                self.first_seen_ns = Some(now_ns);
                self.last_byte_ns = now_ns;
            }
            if self.generation != generation {
                self.generation = generation;
                self.last_byte_ns = last_input_ns.max(self.admitted_ns);
            }
        }
        let ready = if self.canonical || sealed {
            available > 0
        } else {
            match self.policy {
                ReadPolicy::Available | ReadPolicy::FirstByteTimeout { .. } => available > 0,
                ReadPolicy::AtLeast { minimum } => available >= (minimum as usize).min(capacity),
                ReadPolicy::InterByteTimeout { minimum, gap_ns } => {
                    available >= (minimum as usize).min(capacity)
                        || (available > 0 && now_ns >= self.last_byte_ns.saturating_add(gap_ns))
                }
            }
        };
        if ready {
            return ReadDecision::Data(available);
        }
        if now_ns >= self.deadline_ns {
            return ReadDecision::TimedOut;
        }
        if !self.canonical {
            match self.policy {
                ReadPolicy::Available => return ReadDecision::WouldBlock,
                ReadPolicy::FirstByteTimeout { timeout_ns }
                    if now_ns >= self.admitted_ns.saturating_add(timeout_ns) =>
                {
                    return ReadDecision::TimedOut;
                }
                _ => {}
            }
        }
        ReadDecision::Pending
    }

    pub fn next_deadline(&self) -> u64 {
        if self.canonical {
            return self.deadline_ns;
        }
        let policy_deadline = match self.policy {
            ReadPolicy::FirstByteTimeout { timeout_ns } => {
                self.admitted_ns.saturating_add(timeout_ns)
            }
            ReadPolicy::InterByteTimeout { gap_ns, .. } if self.first_seen_ns.is_some() => {
                self.last_byte_ns.saturating_add(gap_ns)
            }
            _ => u64::MAX,
        };
        self.deadline_ns.min(policy_deadline)
    }
}
