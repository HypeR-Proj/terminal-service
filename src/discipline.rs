// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Service-owned line discipline and bounded queues. No blocking operations.
//! The reactor supplies timestamps and commits reads only after their replies
//! have been sent; application clients never instantiate this state.

use std::collections::VecDeque;

use hyper_terminal::{EventFlags, ReadPolicy, Settings, SettingsError};

mod characters;
mod editing;
mod input;
mod output;
mod read;
pub use read::{ReadDecision, ReadRequest};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    pub input_bytes: usize,
    pub line_bytes: usize,
    pub records: usize,
    pub output_bytes: usize,
    pub low_water: usize,
    pub high_water: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            input_bytes: 32768,
            line_bytes: 4096,
            records: 256,
            output_bytes: 65536,
            low_water: 8192,
            high_water: 24576,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Record {
    remaining: usize,
    /// Noncanonical tails can coalesce until a settings transition seals them.
    open: bool,
}

/// All queued bytes remain owned here until the transport commits a read reply.
pub struct Discipline {
    settings: Settings,
    limits: Limits,
    input: VecDeque<u8>,
    records: VecDeque<Record>,
    edit: Vec<u8>,
    output: VecDeque<u8>,
    flow: VecDeque<u8>,
    events: EventFlags,
    literal: bool,
    discard_line: bool,
    received_stop: bool,
    administrative_stop: bool,
    peer_stopped: bool,
    flow_suspended: bool,
    last_input_ns: u64,
    input_generation: u64,
    column: usize,
    edit_start_column: usize,
    erasing: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateError {
    InvalidLimits,
    InvalidSettings(SettingsError),
    Allocation,
}

impl Discipline {
    pub fn new(settings: Settings, limits: Limits) -> Result<Self, CreateError> {
        settings.validate().map_err(CreateError::InvalidSettings)?;
        if limits.line_bytes == 0
            || limits.line_bytes > limits.input_bytes
            || limits.records == 0
            || limits.output_bytes < 16
            || limits.low_water >= limits.high_water
            || limits.high_water > limits.input_bytes
        {
            return Err(CreateError::InvalidLimits);
        }
        let mut this = Self {
            settings,
            limits,
            input: VecDeque::new(),
            records: VecDeque::new(),
            edit: Vec::new(),
            output: VecDeque::new(),
            flow: VecDeque::new(),
            events: EventFlags(0),
            literal: false,
            discard_line: false,
            received_stop: false,
            administrative_stop: false,
            peer_stopped: false,
            flow_suspended: false,
            last_input_ns: 0,
            input_generation: 0,
            column: 0,
            edit_start_column: 0,
            erasing: false,
        };
        this.input
            .try_reserve_exact(limits.input_bytes)
            .map_err(|_| CreateError::Allocation)?;
        this.records
            .try_reserve_exact(limits.records)
            .map_err(|_| CreateError::Allocation)?;
        this.edit
            .try_reserve_exact(limits.line_bytes)
            .map_err(|_| CreateError::Allocation)?;
        this.output
            .try_reserve_exact(limits.output_bytes)
            .map_err(|_| CreateError::Allocation)?;
        this.flow
            .try_reserve_exact(8)
            .map_err(|_| CreateError::Allocation)?;
        Ok(this)
    }

    #[must_use]
    pub fn settings(&self) -> Settings {
        self.settings
    }

    /// No input policy has consumed or transformed these physical bytes. The
    /// service can retain typeahead until the next explicit read/foreground.
    #[must_use]
    pub fn binary_input(&self) -> bool {
        let settings = self.settings;
        !settings.canonical
            && !settings.echo
            && !settings.echo_newline
            && !settings.input.ignore_cr
            && !settings.input.cr_to_lf
            && !settings.input.lf_to_cr
            && !settings.input.strip_high_bit
            && !settings.input.lowercase
            && !settings.input.receive_flow_control
            && !settings.input.send_flow_control
    }

    /// Installs prevalidated settings without reparsing buffered logical bytes.
    /// Seals a noncanonical tail so the old record can be drained even when
    /// subsequent bytes go to untouched raw ingress under a larger minimum.
    /// The reactor cancels pending reads before calling this. If flow characters
    /// change, it must first complete `resume_peer` and the recovery transaction.
    pub fn configure(&mut self, settings: Settings) -> Result<(), SettingsError> {
        settings.validate()?;
        self.finish_erasing();
        if self.settings.canonical && !settings.canonical && !self.edit.is_empty() {
            self.commit_line(false);
        }
        if let Some(last) = self.records.back_mut() {
            last.open = false;
        }
        if !settings.input.receive_flow_control {
            self.received_stop = false;
        }
        if !settings.input.send_flow_control && self.peer_stopped {
            self.resume_peer();
        }
        self.settings = settings;
        self.literal = false;
        self.discard_line = false;
        self.events.0 |= EventFlags::SETTINGS_CHANGED;
        self.update_flow();
        Ok(())
    }

    pub fn clear_input(&mut self) {
        self.input.clear();
        self.records.clear();
        self.edit.clear();
        self.finish_erasing();
        self.literal = false;
        self.discard_line = false;
        self.input_generation = self.input_generation.wrapping_add(1);
        self.update_flow();
    }

    #[must_use]
    pub fn buffered_input(&self) -> usize {
        self.input.len() + self.edit.len()
    }
    /// Includes an empty EOF record, which has no buffered bytes.
    #[must_use]
    pub fn has_read_record(&self) -> bool {
        !self.records.is_empty()
    }
    #[must_use]
    pub fn peer_stopped(&self) -> bool {
        self.peer_stopped
    }
    #[must_use]
    pub fn output_paused(&self) -> bool {
        self.received_stop || self.administrative_stop
    }

    pub fn set_output_paused(&mut self, paused: bool) {
        self.administrative_stop = paused;
    }

    /// Foreground-owner recovery clears both local pause sources. Peer input
    /// flow control has a separate START/fence/recovery-profile transaction.
    pub fn resume_output(&mut self) {
        if self.output_paused() {
            self.events.0 |= EventFlags::OUTPUT_RESUMED;
        }
        self.received_stop = false;
        self.administrative_stop = false;
    }

    /// Priority flow bytes bypass transformed/paused application output. A
    /// caller must fence their handoff before replacing the recovery profile.
    pub fn resume_peer(&mut self) {
        if self.peer_stopped
            && self.flow.len() < 8
            && let Some(start) = self.settings.controls.start
        {
            self.flow.push_back(start);
            self.peer_stopped = false;
        }
    }

    /// Holds automatic STOP generation during an acknowledged profile change.
    pub fn suspend_send_flow(&mut self, suspended: bool) {
        self.flow_suspended = suspended;
        if !suspended {
            self.update_flow();
        }
    }

    pub fn take_events(&mut self) -> EventFlags {
        core::mem::replace(&mut self.events, EventFlags(0))
    }

    fn update_flow(&mut self) {
        if !self.settings.input.send_flow_control || self.flow_suspended {
            return;
        }
        // A canonical reader cannot drain an unfinished line. Throttling that
        // line would prevent the peer from sending its terminating newline.
        let can_drain = !self.settings.canonical || self.has_read_record();
        if can_drain
            && self.buffered_input() >= self.limits.high_water
            && !self.peer_stopped
            && self.flow.len() < 8
        {
            if let Some(stop) = self.settings.controls.stop {
                self.flow.push_back(stop);
                self.peer_stopped = true;
            }
        } else if !can_drain || self.buffered_input() <= self.limits.low_water {
            self.resume_peer();
        }
    }

    /// Commits precisely the response previously obtained by `ReadRequest::poll`.
    /// It must run before another input/read/configuration mutation in the owner.
    pub fn commit_read(&mut self, decision: ReadDecision) {
        match decision {
            ReadDecision::Data(0) => {}
            ReadDecision::Data(count) => {
                self.input.drain(..count.min(self.input.len()));
                if let Some(record) = self.records.front_mut() {
                    record.remaining = record.remaining.saturating_sub(count);
                }
                if self.records.front().is_some_and(|r| r.remaining == 0) {
                    self.records.pop_front();
                }
            }
            ReadDecision::EndOfInput => {
                self.records.pop_front();
            }
            _ => {}
        }
        self.update_flow();
    }
}
