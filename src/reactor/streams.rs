// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl Reactor {
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

    pub(super) fn service_connection(&mut self, index: usize, now: u64) -> Result<()> {
        let connection = &mut self.connections[index];
        connection.blocked_reply = false;
        if let Some(response) = connection.response {
            match connection.channel.as_byte_channel().try_send(&response) {
                Ok(()) => connection.response = None,
                Err(Error::Status(Status::WOULD_BLOCK)) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
        if connection.pending.is_none() || matches!(connection.pending, Some(Pending::Read { .. }))
        {
            let mut frame = [0; stream::MAX_FRAME];
            match connection.channel.as_byte_channel().try_receive(&mut frame) {
                Ok(count) => {
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
                        self.terminal.settings().binary_input() && !self.terminal.has_read_record();
                    let decision = if untouched {
                        // Available samples the upstream raw FIFO once before
                        // reporting no data. A pending read never transfers
                        // staged bytes into a mode-specific logical queue.
                        if request.available_policy()
                            && self.ingress.is_empty()
                            && *sample_generation == self.raw_sample_generation
                        {
                            return Ok(());
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
                        ReadDecision::Pending => return Ok(()),
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
                            return Ok(());
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
        Ok(())
    }
}
