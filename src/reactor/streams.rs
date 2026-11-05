// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Only an uncommitted read is cancellable. A sent DATA response has already
/// removed `pending`; non-read replies and accepted output remain untouched.
fn cancel_uncommitted_read(
    pending: &mut Option<Pending>,
    response: &mut Option<[u8; stream::HEADER_BYTES]>,
) {
    if response.is_none()
        && matches!(pending, Some(Pending::Read { .. }))
        && let Some(Pending::Read { header, .. }) = pending.take()
    {
        *response =
            Some(StreamHeader::response(stream::READ, header.id, stream::CANCELLED, 0, 0).encode());
    }
}

// A conforming sender has one request plus cancellation, but CANCEL is
// idempotent and may be repeated. Exhaustion closes only that stream.
fn drain_input_fence(mut step: impl FnMut() -> Result<bool>) -> Result<()> {
    for _ in 0..64 {
        if !step()? {
            return Ok(());
        }
    }
    Err(Error::Status(Status::RESOURCE_LIMIT))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending_read(id: u64) -> Option<Pending> {
        Some(Pending::Read {
            header: StreamHeader::request(stream::READ, id, 1, u64::MAX),
            request: ReadRequest::new(Settings::raw(), 1, 0, u64::MAX),
            sample_generation: 0,
        })
    }

    #[test]
    fn cutoff_cancels_pending_or_newly_admitted_read_before_data_commit() -> Result<()> {
        for id in [1, 2] {
            let mut pending = pending_read(id);
            let mut response = None;
            cancel_uncommitted_read(&mut pending, &mut response);
            assert!(pending.is_none());
            let header = StreamHeader::decode(&response.ok_or(Error::InvalidResponse)?)?;
            assert_eq!(header.operation, stream::READ | stream::RESPONSE);
            assert_eq!(header.id, id);
            assert_eq!(header.flags, stream::CANCELLED);
            assert_eq!(header.value, 0);
            // Retrying while a cancellation reply is blocked is idempotent.
            let first = response;
            cancel_uncommitted_read(&mut pending, &mut response);
            assert_eq!(response, first);
        }
        Ok(())
    }

    #[test]
    fn cutoff_does_not_replace_committed_data_or_unrelated_output() {
        let mut committed = None;
        let mut response = None;
        cancel_uncommitted_read(&mut committed, &mut response);
        assert!(response.is_none());

        let mut write = Some(Pending::Write {
            header: StreamHeader::request(stream::WRITE, 3, 4, 0),
            count: 4,
        });
        cancel_uncommitted_read(&mut write, &mut response);
        assert!(matches!(write, Some(Pending::Write { count: 4, .. })));
        assert!(response.is_none());

        let output_reply = StreamHeader::response(stream::WRITE, 3, 0, 4, 0).encode();
        response = Some(output_reply);
        cancel_uncommitted_read(&mut committed, &mut response);
        assert_eq!(response, Some(output_reply));
    }

    #[test]
    fn acknowledgement_drains_repeated_cancels_then_cancels_queued_read() -> Result<()> {
        let mut frames = std::collections::VecDeque::from([false, false, false, true]);
        let mut pending = None;
        let mut response = None;
        drain_input_fence(|| {
            let Some(is_read) = frames.pop_front() else {
                return Ok(false);
            };
            if is_read {
                pending = pending_read(4);
            }
            cancel_uncommitted_read(&mut pending, &mut response);
            Ok(true)
        })?;
        assert!(frames.is_empty());
        assert!(pending.is_none());
        assert_eq!(
            StreamHeader::decode(&response.ok_or(Error::InvalidResponse)?)?.flags,
            stream::CANCELLED
        );
        Ok(())
    }

    #[test]
    fn acknowledgement_does_not_wait_for_backpressure_or_unbounded_cancel_flood() {
        let mut calls = 0;
        assert!(
            drain_input_fence(|| {
                calls += 1;
                Ok(false)
            })
            .is_ok()
        );
        assert_eq!(calls, 1);
        calls = 0;
        assert!(matches!(
            drain_input_fence(|| {
                calls += 1;
                Ok(true)
            }),
            Err(Error::Status(Status::RESOURCE_LIMIT))
        ));
        assert_eq!(calls, 64);
    }
}

impl Reactor {
    pub(super) fn local_input_cutoff(&self, id: u64) -> bool {
        self.leases.iter().any(|lease| {
            lease.id == id
                && !matches!(lease.lifetime, LeaseLifetime::Job { .. })
                && lease.event_flags & EventFlags::TERMINATE != 0
        })
    }

    /// Fence submissions that precede the event acknowledgement. A stream has
    /// one outstanding request, but stale CANCEL frames may precede its READ.
    /// Bound the drain and isolate a flooding stream rather than delaying ACK.
    pub(super) fn fence_local_input(&mut self, lease: u64, now: u64) {
        let mut index = 0;
        while index < self.connections.len() {
            if self.connections[index].lease == lease
                && self.connections[index].access & stream::ACCESS_READ != 0
            {
                let failed = drain_input_fence(|| self.service_connection(index, now)).is_err();
                if failed {
                    self.connections.remove(index);
                    continue;
                }
            }
            index += 1;
        }
    }

    pub(super) fn admit(&mut self, lease: u64, accepted: stream::Accepted) {
        let status = if !self.leases.iter().any(|entry| entry.id == lease) {
            Status::CANCELLED
        } else if self.connections.len() == MAX_CONNECTIONS {
            Status::RESOURCE_LIMIT
        } else {
            Status::OK
        };
        let header =
            StreamHeader::response(stream::INFO, 0, 0, stream::INTERACTIVE, status.as_raw());
        if accepted
            .channel
            .as_byte_channel()
            .try_send(&header.encode())
            .is_err()
        {
            return;
        }
        if status == Status::OK {
            self.connections.push(Connection {
                lease,
                channel: accepted.channel,
                access: accepted.access,
                last_id: 0,
                pending: None,
                response: None,
                blocked_reply: false,
                write_buffer: [0; stream::MAX_DATA],
            });
        }
    }

    pub(super) fn service_connection(&mut self, index: usize, now: u64) -> Result<bool> {
        let mut received = false;
        let input_cutoff = self.local_input_cutoff(self.connections[index].lease);
        let connection = &mut self.connections[index];
        connection.blocked_reply = false;
        if let Some(response) = connection.response {
            match connection.channel.as_byte_channel().try_send(&response) {
                Ok(()) => connection.response = None,
                Err(Error::Status(Status::WOULD_BLOCK)) => return Ok(received),
                Err(error) => return Err(error),
            }
        }
        if connection.pending.is_none() || matches!(connection.pending, Some(Pending::Read { .. }))
        {
            let mut frame = [0; stream::MAX_FRAME];
            match connection.channel.as_byte_channel().try_receive(&mut frame) {
                Ok(count) => {
                    received = true;
                    let header = StreamHeader::decode(&frame[..count])?;
                    if header.flags != 0 || header.operation & stream::RESPONSE != 0 {
                        return Err(Error::InvalidResponse);
                    }
                    if header.operation == stream::CANCEL {
                        if count != stream::HEADER_BYTES
                            || header.id == 0
                            || header.value != header.id
                            || header.extra != 0
                        {
                            return Err(Error::InvalidResponse);
                        }
                        if let Some(Pending::Read {
                            header: pending, ..
                        }) = &connection.pending
                            && pending.id == header.id
                        {
                            connection.response = Some(
                                StreamHeader::response(
                                    stream::READ,
                                    header.id,
                                    stream::CANCELLED,
                                    0,
                                    0,
                                )
                                .encode(),
                            );
                            connection.pending = None;
                        }
                    } else {
                        if header.id <= connection.last_id || connection.pending.is_some() {
                            return Err(Error::InvalidResponse);
                        }
                        connection.last_id = header.id;
                        connection.pending = Some(match header.operation {
                            stream::READ
                                if connection.access & stream::ACCESS_READ != 0
                                    && header.value > 0
                                    && header.value <= stream::MAX_DATA as u64
                                    && count == stream::HEADER_BYTES =>
                            {
                                let settings = self
                                    .leases
                                    .iter()
                                    .find(|lease| lease.id == connection.lease)
                                    .ok_or(Error::InvalidResponse)?
                                    .settings;
                                Pending::Read {
                                    header,
                                    sample_generation: self.raw_sample_generation,
                                    request: ReadRequest::new(
                                        settings,
                                        header.value as usize,
                                        now,
                                        header.extra,
                                    ),
                                }
                            }
                            stream::WRITE
                                if connection.access & stream::ACCESS_WRITE != 0
                                    && header.value > 0
                                    && header.value <= stream::MAX_DATA as u64
                                    && count == stream::HEADER_BYTES + header.value as usize
                                    && header.extra == 0 =>
                            {
                                connection.write_buffer[..header.value as usize]
                                    .copy_from_slice(&frame[stream::HEADER_BYTES..count]);
                                Pending::Write {
                                    header,
                                    count: header.value as usize,
                                }
                            }
                            stream::FENCE
                                if connection.access & stream::ACCESS_WRITE != 0
                                    && count == stream::HEADER_BYTES
                                    && header.value == 0
                                    && header.extra == 0 =>
                            {
                                Pending::Fence {
                                    header,
                                    target: self.drained_output + self.terminal.output_len() as u64,
                                }
                            }
                            _ => return Err(Error::InvalidResponse),
                        });
                    }
                }
                Err(Error::Status(Status::WOULD_BLOCK)) => {}
                Err(error) => return Err(error),
            }
        }
        let connection = &mut self.connections[index];
        if input_cutoff {
            cancel_uncommitted_read(&mut connection.pending, &mut connection.response);
        }
        let mut complete = false;
        if let Some(pending) = &mut connection.pending {
            match pending {
                Pending::Read {
                    header,
                    request,
                    sample_generation,
                } if connection.lease == self.foreground => {
                    let mut frame = [0; stream::MAX_FRAME];
                    let untouched =
                        self.terminal.binary_input() && !self.terminal.has_read_record();
                    let decision = if untouched {
                        // Available samples the upstream raw FIFO once before
                        // reporting no data. A pending read never transfers
                        // staged bytes into a mode-specific logical queue.
                        if request.available_policy()
                            && self.ingress.is_empty()
                            && *sample_generation == self.raw_sample_generation
                        {
                            return Ok(received);
                        }
                        let decision = request.poll_raw(
                            self.ingress.len(),
                            self.ingress_time,
                            self.ingress_generation,
                            now,
                        );
                        if let ReadDecision::Data(count) = decision {
                            for (destination, source) in frame[stream::HEADER_BYTES..]
                                .iter_mut()
                                .zip(self.ingress.iter())
                                .take(count)
                            {
                                *destination = *source;
                            }
                        }
                        decision
                    } else {
                        request.poll(&self.terminal, now, &mut frame[stream::HEADER_BYTES..])
                    };
                    let (flags, count, status) = match decision {
                        ReadDecision::Pending => return Ok(received),
                        ReadDecision::Data(count) => (stream::DATA, count, Status::OK),
                        ReadDecision::EndOfInput => (stream::EOF, 0, Status::OK),
                        ReadDecision::TimedOut => (stream::TIMED_OUT, 0, Status::OK),
                        ReadDecision::WouldBlock => (0, 0, Status::WOULD_BLOCK),
                    };
                    frame[..stream::HEADER_BYTES].copy_from_slice(
                        &StreamHeader::response(
                            stream::READ,
                            header.id,
                            flags,
                            count as u64,
                            status.as_raw(),
                        )
                        .encode(),
                    );
                    match connection
                        .channel
                        .as_byte_channel()
                        .try_send(&frame[..stream::HEADER_BYTES + count])
                    {
                        Ok(()) => {
                            if untouched {
                                if let ReadDecision::Data(count) = decision {
                                    self.ingress.drain(..count);
                                }
                            } else {
                                self.terminal.commit_read(decision);
                            }
                            complete = true;
                        }
                        Err(Error::Status(Status::WOULD_BLOCK)) => connection.blocked_reply = true,
                        Err(error) => return Err(error),
                    }
                }
                Pending::Write { header, count } => {
                    if let Some(lease) = self
                        .leases
                        .iter_mut()
                        .find(|lease| lease.id == connection.lease)
                    {
                        // Loader diagnostics precede runtime readiness. Waiting
                        // for bind here would deadlock the launcher that needs
                        // readiness before it obtains the Process observer.
                        let maximum = if lease.activated {
                            *count
                        } else {
                            (*count).min(lease.startup_output_remaining)
                        };
                        if maximum == 0 {
                            connection.response = Some(
                                StreamHeader::response(
                                    stream::WRITE,
                                    header.id,
                                    0,
                                    0,
                                    Status::RESOURCE_LIMIT.as_raw(),
                                )
                                .encode(),
                            );
                            connection.pending = None;
                            return Ok(received);
                        }
                        let accepted = self
                            .terminal
                            .write_output(&connection.write_buffer[..maximum]);
                        if accepted > 0 {
                            if !lease.activated {
                                lease.startup_output_remaining -= accepted;
                            }
                            connection.response = Some(
                                StreamHeader::response(
                                    stream::WRITE,
                                    header.id,
                                    0,
                                    accepted as u64,
                                    0,
                                )
                                .encode(),
                            );
                            complete = true;
                        } else if !lease.activated {
                            // A loader must not wait for activation while the
                            // launcher's existing output has exhausted space.
                            connection.response = Some(
                                StreamHeader::response(
                                    stream::WRITE,
                                    header.id,
                                    0,
                                    0,
                                    Status::RESOURCE_LIMIT.as_raw(),
                                )
                                .encode(),
                            );
                            complete = true;
                        }
                    }
                }
                Pending::Fence { header, target } if *target <= self.fenced_output => {
                    connection.response =
                        Some(StreamHeader::response(stream::FENCE, header.id, 0, 0, 0).encode());
                    complete = true;
                }
                Pending::Fence { header, .. }
                    if self.terminal.output_paused()
                        && self
                            .leases
                            .iter()
                            .any(|lease| lease.id == connection.lease && !lease.activated) =>
                {
                    // A successful fence still means queue handoff; an
                    // inactive loader gets an error instead of depending on
                    // activation to release another client's output pause.
                    connection.response = Some(
                        StreamHeader::response(
                            stream::FENCE,
                            header.id,
                            0,
                            0,
                            Status::WOULD_BLOCK.as_raw(),
                        )
                        .encode(),
                    );
                    complete = true;
                }
                _ => {}
            }
        }
        if complete {
            connection.pending = None;
        }
        Ok(received)
    }
}
