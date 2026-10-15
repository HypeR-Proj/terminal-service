// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! The only owner of terminal queues, settings and foreground transitions.

use std::collections::VecDeque;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};

use hyper_os::capability_channel::CapabilityChannel;
use hyper_os::handle::{ByteChannelObject, OwnedHandle, ProcessObject};
use hyper_os::startup::Startup;
use hyper_os::stream::{self, Header as StreamHeader};
use hyper_os::wait::{ObjectSignals, WaitItem, wait_many};
use hyper_os::{Error, Result, Status};
use hyper_terminal::client;
use hyper_terminal::protocol;
use hyper_terminal::{Discipline, EventFlags, Limits, ReadDecision, ReadRequest, Settings};

use crate::admission::{self, ControlRequest, PreparedHandles, Publisher, Work};

const MAX_LEASES: usize = 8;
const MAX_CONNECTIONS: usize = 24;

struct Lease {
    id: u64,
    parent: u64,
    alive: Arc<AtomicBool>,
    owner: Option<OwnedHandle<ProcessObject>>,
    activated: bool,
    startup_output_remaining: usize,
    settings: Settings,
    events: OwnedHandle<ByteChannelObject>,
    event_flags: u32,
    presentation: u64,
}

struct Connection {
    lease: u64,
    channel: OwnedHandle<ByteChannelObject>,
    access: u64,
    last_id: u64,
    pending: Option<Pending>,
    response: Option<[u8; stream::HEADER_BYTES]>,
    blocked_reply: bool,
    write_buffer: [u8; stream::MAX_DATA],
}

enum Pending {
    Read {
        header: StreamHeader,
        request: ReadRequest,
        sample_generation: u64,
    },
    Write {
        header: StreamHeader,
        count: usize,
    },
    Fence {
        header: StreamHeader,
        target: u64,
    },
}

#[derive(Clone, Copy)]
enum RawPending {
    Write { count: usize, flow: bool },
    Fence { through: u64, configuration: bool },
}

struct Configuration {
    request: Option<ControlRequest>,
    target: u64,
    reset_presentation: bool,
    settings: Settings,
    generation: u64,
    phase: ConfigurationPhase,
    deadline: u64,
}
#[derive(Clone, Copy, PartialEq)]
enum ConfigurationPhase {
    ResumePeer,
    Fencing,
    NotifyManager,
    AwaitManager,
}

struct InputFlush {
    request: ControlRequest,
    phase: InputFlushPhase,
    id: u64,
}
#[derive(Clone, Copy, PartialEq)]
enum InputFlushPhase {
    Cancelling,
    SendDiscard,
    AwaitDiscard,
}

struct Reactor {
    terminal: Discipline,
    leases: Vec<Lease>,
    connections: Vec<Connection>,
    foreground: u64,
    next_lease: u64,
    columns: u16,
    rows: u16,
    raw_input: stream::Connection,
    raw_output: stream::Connection,
    raw_control: stream::Connection,
    ingress: VecDeque<u8>,
    ingress_time: u64,
    ingress_generation: u64,
    raw_sample_generation: u64,
    raw_read_capacity: usize,
    raw_read_deadline: u64,
    raw_cancel_sent: bool,
    recovery: OwnedHandle<ByteChannelObject>,
    recovery_generation: u64,
    configuration: Option<Configuration>,
    input_flush: Option<InputFlush>,
    deferred: Vec<ControlRequest>,
    raw_pending: Option<RawPending>,
    raw_send_blocked: bool,
    raw_id: u64,
    raw_pending_id: u64,
    drained_output: u64,
    fenced_output: u64,
    publisher: Publisher,
    receiver: mpsc::Receiver<Work>,
    wake: OwnedHandle<ByteChannelObject>,
}

pub fn run(startup: &mut Startup<'_>) -> Result<()> {
    let management = CapabilityChannel::from_handle(startup.take(client::MANAGEMENT)?);
    let ready = startup.take(client::READY)?;
    let raw_input_owner = startup.take(client::RAW_INPUT)?;
    let raw_output_owner = startup.take(client::RAW_OUTPUT)?;
    let recovery = startup.take(client::RECOVERY)?;
    let raw_input = stream::connect(raw_input_owner.as_handle_ref(), stream::ACCESS_READ)?;
    let raw_output = stream::connect(raw_output_owner.as_handle_ref(), stream::ACCESS_WRITE)?;
    let raw_control = stream::connect(raw_input_owner.as_handle_ref(), stream::ACCESS_READ)?;
    let (wake, wake_sender) = hyper_os::channel::create_pair()?;
    let (sender, receiver) = mpsc::sync_channel(MAX_LEASES * 4);
    let publisher = Publisher::new(sender, wake_sender);
    admission::control_worker(
        0,
        management,
        publisher.clone(),
        Arc::new(AtomicBool::new(true)),
    )?;
    let mut reactor = Reactor {
        terminal: Discipline::new(Settings::default(), Limits::default())
            .map_err(|_| Error::Status(Status::NO_MEMORY))?,
        leases: Vec::new(),
        connections: Vec::new(),
        foreground: 0,
        next_lease: 1,
        columns: 80,
        rows: 24,
        raw_input,
        raw_output,
        raw_control,
        ingress: VecDeque::new(),
        ingress_time: 0,
        ingress_generation: 0,
        raw_sample_generation: 0,
        raw_read_capacity: 0,
        raw_read_deadline: hyper_os::DEADLINE_INFINITE,
        raw_cancel_sent: false,
        recovery,
        recovery_generation: 0,
        configuration: None,
        input_flush: None,
        deferred: Vec::new(),
        raw_pending: None,
        raw_send_blocked: false,
        raw_id: 1,
        raw_pending_id: 0,
        drained_output: 0,
        fenced_output: 0,
        publisher,
        receiver,
        wake,
    };
    reactor
        .ingress
        .try_reserve_exact(stream::MAX_DATA)
        .map_err(|_| Error::Status(Status::NO_MEMORY))?;
    reactor
        .leases
        .try_reserve_exact(MAX_LEASES)
        .map_err(|_| Error::Status(Status::NO_MEMORY))?;
    reactor
        .connections
        .try_reserve_exact(MAX_CONNECTIONS)
        .map_err(|_| Error::Status(Status::NO_MEMORY))?;
    reactor
        .deferred
        .try_reserve_exact(MAX_LEASES)
        .map_err(|_| Error::Status(Status::NO_MEMORY))?;
    ready.as_byte_channel().send(b"terminal-ready\n")?;
    loop {
        reactor.step()?;
    }
}

mod configuration;
mod control;
mod leases;
mod raw;
mod streams;

impl Reactor {
    fn step(&mut self) -> Result<()> {
        let now = hyper_os::time::monotonic_now()?.as_nanoseconds();
        let mut wake_bytes = [0; 32];
        for _ in 0..32 {
            if self
                .wake
                .as_byte_channel()
                .try_receive(&mut wake_bytes)
                .is_err()
            {
                break;
            }
        }
        for _ in 0..MAX_LEASES * 4 {
            match self.receiver.try_recv() {
                Ok(Work::Stream { lease, accepted }) => self.admit(lease, accepted),
                Ok(Work::Control(request)) => self.control(request)?,
                Ok(Work::ControlClosed { lease: 0 }) => {
                    return Err(Error::Status(Status::PEER_CLOSED));
                }
                Ok(Work::ControlClosed { .. }) => {
                    // Applications may discard unused control capabilities
                    // before std cleanup finishes. Data authority belongs to
                    // the observed Process, not this optional control handle.
                    // Dormant leases remain bounded and explicitly releasable
                    // by their creator until the Process observer is bound.
                }
                Err(_) => break,
            }
        }
        self.retire_owners(now)?;
        let mut index = 0;
        while index < self.deferred.len() {
            if self
                .leases
                .iter()
                .any(|lease| lease.id == self.deferred[index].lease && !lease.activated)
            {
                index += 1;
            } else {
                let request = self.deferred.remove(index);
                self.control(request)?;
            }
        }
        let mut input = [0; stream::MAX_DATA];
        if self.raw_read_capacity != 0
            && !self
                .input_flush
                .as_ref()
                .is_some_and(|flush| flush.phase != InputFlushPhase::Cancelling)
            && let Some(result) = self.raw_input.try_complete(&mut input)?
        {
            match result {
                stream::ReadResult::Data(count) if self.input_flush.is_none() => {
                    if count > stream::MAX_DATA - self.ingress.len() {
                        return Err(Error::InvalidResponse);
                    }
                    self.ingress.extend(&input[..count]);
                    self.ingress_time = now;
                    self.ingress_generation = self.ingress_generation.wrapping_add(1);
                    self.raw_sample_generation = self.raw_sample_generation.wrapping_add(1);
                }
                stream::ReadResult::TimedOut => {
                    self.raw_sample_generation = self.raw_sample_generation.wrapping_add(1);
                }
                stream::ReadResult::Eof => return Err(Error::Status(Status::PEER_CLOSED)),
                _ => {}
            }
            self.raw_read_capacity = 0;
            self.raw_cancel_sent = false;
            if let Some(flush) = self.input_flush.as_mut() {
                flush.phase = InputFlushPhase::SendDiscard;
            }
        }
        self.finish_raw()?;
        self.finish_input_flush()?;
        self.configure_progress(now)?;
        if self.foreground != 0
            && self.input_flush.is_none()
            && !self.terminal.settings().binary_input()
        {
            self.feed_cooked_ingress(now);
        }
        let mut index = 0;
        while index < self.connections.len() {
            if self.service_connection(index, now).is_err() {
                self.connections.remove(index);
            } else {
                index += 1;
            }
        }
        if !self.connections.is_empty() {
            self.connections.rotate_left(1);
        }
        self.arm_input(now)?;
        self.submit_raw()?;
        let flags = self.terminal.take_events().0;
        if flags != 0 {
            self.notify(flags);
        }
        let mut items = Vec::new();
        items
            .try_reserve_exact(MAX_CONNECTIONS + MAX_LEASES + 5)
            .map_err(|_| Error::Status(Status::NO_MEMORY))?;
        items.push(self.raw_input.wait_item());
        items.push(WaitItem::new(
            self.wake.as_handle_ref(),
            ObjectSignals::<ByteChannelObject>::READABLE,
        ));
        let raw_output_index = items.len();
        let output_signals = if self.raw_pending.is_some() {
            ObjectSignals::<ByteChannelObject>::READABLE
        } else if self.raw_send_blocked {
            ObjectSignals::<ByteChannelObject>::WRITABLE
        } else {
            ObjectSignals::<ByteChannelObject>::PEER_CLOSED
        };
        items.push(WaitItem::new(
            self.raw_output.as_handle_ref(),
            output_signals.union(ObjectSignals::<ByteChannelObject>::PEER_CLOSED),
        ));
        let raw_control_index = items.len();
        let control_signals = match self.input_flush.as_ref().map(|flush| flush.phase) {
            Some(InputFlushPhase::AwaitDiscard) => ObjectSignals::<ByteChannelObject>::READABLE,
            Some(InputFlushPhase::SendDiscard) => ObjectSignals::<ByteChannelObject>::WRITABLE,
            _ => ObjectSignals::<ByteChannelObject>::PEER_CLOSED,
        };
        items.push(WaitItem::new(
            self.raw_control.as_handle_ref(),
            control_signals.union(ObjectSignals::<ByteChannelObject>::PEER_CLOSED),
        ));
        if let Some(config) = &self.configuration {
            let signals = match config.phase {
                ConfigurationPhase::AwaitManager => {
                    Some(ObjectSignals::<ByteChannelObject>::READABLE)
                }
                ConfigurationPhase::NotifyManager => {
                    Some(ObjectSignals::<ByteChannelObject>::WRITABLE)
                }
                _ => None,
            };
            if let Some(signals) = signals {
                items.push(WaitItem::new(
                    self.recovery.as_handle_ref(),
                    signals.union(ObjectSignals::<ByteChannelObject>::PEER_CLOSED),
                ));
            }
        }
        let mut deadline = hyper_os::DEADLINE_INFINITE;
        if let Some(config) = &self.configuration {
            deadline = config.deadline;
        }
        for connection in &self.connections {
            let mut signals = ObjectSignals::<ByteChannelObject>::PEER_CLOSED;
            if connection.response.is_some() || connection.blocked_reply {
                signals = signals.union(ObjectSignals::<ByteChannelObject>::WRITABLE);
            }
            // A pending read still accepts cancellation. Other operations are
            // serialized and cannot spin on a queued later request.
            if connection.response.is_none()
                && (connection.pending.is_none()
                    || matches!(connection.pending, Some(Pending::Read { .. })))
            {
                signals = signals.union(ObjectSignals::<ByteChannelObject>::READABLE);
            }
            items.push(WaitItem::new(connection.channel.as_handle_ref(), signals));
            if connection.lease == self.foreground
                && !connection.blocked_reply
                && connection.response.is_none()
                && let Some(Pending::Read { request, .. }) = &connection.pending
            {
                // A ready read cannot make progress until its reply fits.
                // Its expired deadline must not turn backpressure into a spin.
                deadline = deadline.min(request.next_deadline());
            }
        }
        for lease in &self.leases {
            if let Some(owner) = &lease.owner {
                items.push(WaitItem::new(
                    owner.as_handle_ref(),
                    ObjectSignals::<ProcessObject>::TERMINATED,
                ));
            }
        }
        match wait_many(&items, deadline) {
            Ok(observation)
                if [0, raw_output_index, raw_control_index].contains(&observation.index)
                    && ObjectSignals::<ByteChannelObject>::PEER_CLOSED
                        .is_present_in(observation.observed) =>
            {
                // An idle demand-driven input reader performs no receive call
                // to discover closure. Retire the whole raw binding instead
                // of repeatedly waking on its permanently closed endpoint.
                Err(Error::Status(Status::PEER_CLOSED))
            }
            Ok(_) | Err(Error::Status(Status::TIMED_OUT)) => Ok(()),
            Err(error) => Err(error),
        }
    }
}
