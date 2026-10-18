// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Capability rendezvous blocks only bounded admission workers. They never
//! mutate terminal state or wait on application data reads.

use std::mem::MaybeUninit;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
};
use std::time::Duration;

use hyper_os::capability_channel::{
    CapabilityChannel, CapabilityDisposition, CapabilityReceiveSlot,
};
use hyper_os::handle::{
    ByteChannelObject, CapabilityChannelObject, OwnedHandle, ProcessObject, RightsOffer,
};
use hyper_os::stream;
use hyper_os::wait::{ObjectSignals, WaitItem, wait_many};
use hyper_os::{Error, Result, Status};
use hyper_terminal::client::{CONTROL_RIGHTS, EVENT_RIGHTS, OBSERVER_RIGHTS};
use hyper_terminal::protocol::{self, Header};

pub enum Work {
    Stream {
        lease: u64,
        accepted: stream::Accepted,
    },
    Control(ControlRequest),
    ControlClosed {
        lease: u64,
    },
}

pub struct ControlRequest {
    pub lease: u64,
    pub header: Header,
    pub payload: [u8; protocol::SETTINGS_BYTES],
    pub observer: Option<OwnedHandle<ProcessObject>>,
    pub reply: mpsc::SyncSender<ControlReply>,
}

pub struct PreparedHandles {
    pub stream: Option<OwnedHandle<CapabilityChannelObject>>,
    pub control: Option<OwnedHandle<CapabilityChannelObject>>,
    pub events: Option<OwnedHandle<ByteChannelObject>>,
}

pub struct ControlReply {
    pub header: Header,
    pub payload: Option<[u8; protocol::SETTINGS_BYTES]>,
    pub prepared: Option<PreparedHandles>,
}

impl ControlRequest {
    pub fn respond(
        self,
        status: Status,
        value: u64,
        payload: Option<[u8; protocol::SETTINGS_BYTES]>,
        prepared: Option<PreparedHandles>,
    ) {
        let _ = self.reply.try_send(ControlReply {
            header: Header {
                operation: self.header.operation | protocol::RESPONSE,
                id: self.header.id,
                value,
                status: status.as_raw(),
            },
            payload,
            prepared,
        });
    }
}

#[derive(Clone)]
pub struct Publisher {
    sender: mpsc::SyncSender<Work>,
    wake: Arc<OwnedHandle<ByteChannelObject>>,
    workers: Arc<AtomicUsize>,
}

impl Publisher {
    pub fn new(sender: mpsc::SyncSender<Work>, wake: OwnedHandle<ByteChannelObject>) -> Self {
        Self {
            sender,
            wake: Arc::new(wake),
            workers: Arc::new(AtomicUsize::new(0)),
        }
    }
    fn publish(&self, work: Work) -> bool {
        if self.sender.send(work).is_err() {
            return false;
        }
        // A full wake queue already guarantees a pending reactor notification.
        let _ = self.wake.as_byte_channel().try_send(&[1]);
        true
    }

    fn reserve_worker(&self) -> Result<WorkerReservation> {
        // Count retiring workers too: repeatedly preparing and releasing a
        // dormant lease must not evade the live-lease bound during timeout exit.
        self.workers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < 17).then_some(count + 1)
            })
            .map_err(|_| Error::Status(Status::RESOURCE_LIMIT))?;
        Ok(WorkerReservation(self.workers.clone()))
    }
}

struct WorkerReservation(Arc<AtomicUsize>);
impl Drop for WorkerReservation {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

pub fn control_worker(
    lease: u64,
    listener: CapabilityChannel,
    publisher: Publisher,
    alive: Arc<AtomicBool>,
) -> Result<()> {
    let reservation = publisher.reserve_worker()?;
    std::thread::Builder::new()
        .name(format!("terminal-control/{lease}"))
        .spawn(move || {
            let _reservation = reservation;
            let _closed = ClosedControl {
                lease,
                publisher: publisher.clone(),
            };
            while alive.load(Ordering::Acquire) {
                let mut bytes = [MaybeUninit::uninit(); protocol::MAX_MESSAGE];
                let mut slots = [CapabilityReceiveSlot::new::<ProcessObject>(OBSERVER_RIGHTS)];
                let deadline = deadline();
                let message = match listener.receive(deadline, &mut bytes, &mut slots) {
                    Ok(message) => message,
                    Err(Error::Status(Status::TIMED_OUT)) => continue,
                    Err(_) => break,
                };
                let Some(header) = Header::decode(message.bytes()) else {
                    break;
                };
                let expected = if header.operation == protocol::SET_SETTINGS {
                    protocol::SETTINGS_BYTES
                } else {
                    0
                };
                if header.status != 0
                    || message.bytes().len() != protocol::HEADER_BYTES + expected
                    || message.capability_count() != usize::from(header.operation == protocol::BIND)
                {
                    break;
                }
                let mut payload = [0; protocol::SETTINGS_BYTES];
                payload[..expected].copy_from_slice(&message.bytes()[protocol::HEADER_BYTES..]);
                let observer = match slots[0].take() {
                    Ok(observer) => observer,
                    Err(_) => break,
                };
                let (reply, response) = mpsc::sync_channel(1);
                if !publisher.publish(Work::Control(ControlRequest {
                    lease,
                    header,
                    payload,
                    observer,
                    reply,
                })) {
                    break;
                }
                loop {
                    match response.recv_timeout(Duration::from_secs(1)) {
                        Ok(mut response) => {
                            if send_reply(&listener, &alive, &mut response).is_err() {
                                return;
                            }
                            break;
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) if alive.load(Ordering::Acquire) => {}
                        Err(_) => return,
                    }
                }
            }
        })
        .map_err(|_| Error::Status(Status::NO_MEMORY))?;
    Ok(())
}

struct ClosedControl {
    lease: u64,
    publisher: Publisher,
}
impl Drop for ClosedControl {
    fn drop(&mut self) {
        let _ = self
            .publisher
            .publish(Work::ControlClosed { lease: self.lease });
    }
}

pub fn stream_worker(
    lease: u64,
    listener: CapabilityChannel,
    publisher: Publisher,
    alive: Arc<AtomicBool>,
) -> Result<()> {
    let reservation = publisher.reserve_worker()?;
    std::thread::Builder::new()
        .name(format!("terminal-stream/{lease}"))
        .spawn(move || {
            let _reservation = reservation;
            while alive.load(Ordering::Acquire) {
                match stream::accept_until(&listener, deadline()) {
                    Ok(accepted) => {
                        if !publisher.publish(Work::Stream { lease, accepted }) {
                            break;
                        }
                    }
                    Err(Error::Status(Status::TIMED_OUT))
                    | Err(Error::Status(Status::ACCESS_DENIED))
                    | Err(Error::Status(Status::BAD_HANDLE))
                    | Err(Error::Status(Status::INVALID_ARGUMENT))
                    | Err(Error::InvalidResponse)
                    | Err(Error::UnexpectedObjectKind { .. })
                    | Err(Error::MessageTooLarge { .. }) => {}
                    Err(_) => break,
                }
            }
        })
        .map_err(|_| Error::Status(Status::NO_MEMORY))?;
    Ok(())
}

fn deadline() -> u64 {
    hyper_os::time::monotonic_now()
        .map_or(0, |now| now.as_nanoseconds().saturating_add(1_000_000_000))
}

fn send_reply(
    listener: &CapabilityChannel,
    alive: &AtomicBool,
    reply: &mut ControlReply,
) -> Result<()> {
    let mut bytes = [0; protocol::MAX_MESSAGE];
    bytes[..protocol::HEADER_BYTES].copy_from_slice(&reply.header.encode());
    let length = if let Some(payload) = reply.payload {
        bytes[protocol::HEADER_BYTES..].copy_from_slice(&payload);
        protocol::MAX_MESSAGE
    } else {
        protocol::HEADER_BYTES
    };
    while alive.load(Ordering::Acquire) {
        let result = if let Some(prepared) = reply.prepared.as_mut() {
            let mut offers = [
                CapabilityDisposition::move_handle(
                    &mut prepared.stream,
                    RightsOffer::Exact(stream::CONNECTOR_RIGHTS),
                )?,
                CapabilityDisposition::move_handle(
                    &mut prepared.control,
                    RightsOffer::Exact(CONTROL_RIGHTS),
                )?,
                CapabilityDisposition::move_handle(
                    &mut prepared.events,
                    RightsOffer::Exact(EVENT_RIGHTS),
                )?,
            ];
            listener.try_send(&bytes[..length], &mut offers)
        } else {
            listener.try_send(&bytes[..length], &mut [])
        };
        match result {
            Ok(()) => return Ok(()),
            Err(Error::Status(Status::WOULD_BLOCK)) => {
                match wait_many(
                    &[WaitItem::new(
                        listener.as_handle_ref(),
                        ObjectSignals::<CapabilityChannelObject>::PEER_RECEIVING
                            .union(ObjectSignals::<CapabilityChannelObject>::PEER_CLOSED),
                    )],
                    deadline(),
                ) {
                    Ok(_) | Err(Error::Status(Status::TIMED_OUT)) => {}
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        }
    }
    Err(Error::Status(Status::CANCELLED))
}
