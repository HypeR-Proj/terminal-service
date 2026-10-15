// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl Reactor {
    pub(super) fn feed_cooked_ingress(&mut self, now: u64) {
        let mut bytes = [0; stream::MAX_DATA];
        let count = self.ingress.len();
        for (destination, source) in bytes.iter_mut().zip(self.ingress.drain(..)) {
            *destination = source;
        }
        self.terminal.input(&bytes[..count], now);
    }

    pub(super) fn arm_input(&mut self, now: u64) -> Result<()> {
        if self.input_flush.is_some() {
            return Ok(());
        }
        let binary = self.terminal.binary_input();
        let demand = if self.foreground == 0 {
            None
        } else if binary {
            self.connections.iter().find_map(|connection| {
                if connection.lease != self.foreground
                    || connection.blocked_reply
                    || connection.response.is_some()
                {
                    return None;
                }
                let Some(Pending::Read { request, .. }) = &connection.pending else {
                    return None;
                };
                Some((
                    request.capacity.saturating_sub(self.ingress.len()),
                    if request.available_policy() {
                        now
                    } else {
                        request.next_deadline()
                    },
                ))
            })
        } else {
            Some((
                stream::MAX_DATA - self.ingress.len(),
                hyper_os::DEADLINE_INFINITE,
            ))
        };
        let (capacity, deadline) = demand.unwrap_or((0, hyper_os::DEADLINE_INFINITE));
        let capacity = capacity.min(stream::MAX_DATA - self.ingress.len());
        if self.raw_read_capacity != 0 {
            if !self.raw_cancel_sent
                && (capacity == 0
                    || (binary && self.raw_read_capacity > capacity)
                    || (deadline <= now && self.raw_read_deadline > now))
            {
                self.raw_input.cancel()?;
                self.raw_cancel_sent = true;
            }
        } else if capacity > 0 {
            self.raw_input.start_read(capacity, deadline)?;
            self.raw_read_capacity = capacity;
            self.raw_read_deadline = deadline;
        }
        Ok(())
    }

    pub(super) fn finish_raw(&mut self) -> Result<()> {
        let Some(pending) = self.raw_pending else {
            return Ok(());
        };
        let mut bytes = [0; stream::HEADER_BYTES];
        let count = match self.raw_output.as_byte_channel().try_receive(&mut bytes) {
            Ok(count) => count,
            Err(Error::Status(Status::WOULD_BLOCK)) => return Ok(()),
            Err(error) => return Err(error),
        };
        let header = StreamHeader::decode(&bytes[..count])?;
        if count != stream::HEADER_BYTES || header.id != self.raw_pending_id || header.flags != 0 {
            return Err(Error::InvalidResponse);
        }
        header.status()?;
        match pending {
            RawPending::Write { count, flow } => {
                let accepted = usize::try_from(header.value).map_err(|_| Error::InvalidResponse)?;
                if header.operation != stream::WRITE | stream::RESPONSE
                    || accepted == 0
                    || accepted > count
                {
                    return Err(Error::InvalidResponse);
                }
                if flow {
                    self.terminal.commit_flow_output(accepted);
                } else {
                    self.terminal.commit_output(accepted);
                    self.drained_output += accepted as u64;
                }
            }
            RawPending::Fence {
                through,
                configuration,
            } => {
                if header.operation != stream::FENCE | stream::RESPONSE {
                    return Err(Error::InvalidResponse);
                }
                self.fenced_output = through;
                if configuration && let Some(config) = self.configuration.as_mut() {
                    config.phase = ConfigurationPhase::NotifyManager;
                }
            }
        }
        self.raw_pending = None;
        Ok(())
    }

    pub(super) fn submit_raw(&mut self) -> Result<()> {
        if self.raw_pending.is_some() {
            return Ok(());
        }
        if self
            .configuration
            .as_ref()
            .is_some_and(|config| config.phase == ConfigurationPhase::ResumePeer)
        {
            self.terminal.resume_peer();
        }
        let target = self
            .connections
            .iter()
            .filter_map(|connection| match connection.pending {
                Some(Pending::Fence { target, .. }) if target > self.fenced_output => Some(target),
                _ => None,
            })
            .min();
        let mut frame = [0; stream::MAX_FRAME];
        let flow_count = self
            .terminal
            .flow_output(&mut frame[stream::HEADER_BYTES..]);
        let (header, pending, length) = if flow_count > 0 {
            (
                StreamHeader::request(stream::WRITE, self.raw_id, flow_count as u64, 0),
                RawPending::Write {
                    count: flow_count,
                    flow: true,
                },
                stream::HEADER_BYTES + flow_count,
            )
        } else if target.is_some_and(|target| target <= self.drained_output)
            || self
                .configuration
                .as_ref()
                .is_some_and(|config| config.phase == ConfigurationPhase::ResumePeer)
        {
            let configuration = self
                .configuration
                .as_ref()
                .is_some_and(|config| config.phase == ConfigurationPhase::ResumePeer);
            (
                StreamHeader::request(stream::FENCE, self.raw_id, 0, 0),
                RawPending::Fence {
                    through: self.drained_output,
                    configuration,
                },
                stream::HEADER_BYTES,
            )
        } else {
            let count = self.terminal.output(&mut frame[stream::HEADER_BYTES..]);
            if count == 0 {
                return Ok(());
            }
            (
                StreamHeader::request(stream::WRITE, self.raw_id, count as u64, 0),
                RawPending::Write { count, flow: false },
                stream::HEADER_BYTES + count,
            )
        };
        frame[..stream::HEADER_BYTES].copy_from_slice(&header.encode());
        match self.raw_output.as_byte_channel().try_send(&frame[..length]) {
            Ok(()) => self.raw_send_blocked = false,
            Err(Error::Status(Status::WOULD_BLOCK)) => {
                self.raw_send_blocked = true;
                return Ok(());
            }
            Err(error) => return Err(error),
        }
        if matches!(
            pending,
            RawPending::Fence {
                configuration: true,
                ..
            }
        ) && let Some(config) = self.configuration.as_mut()
        {
            config.phase = ConfigurationPhase::Fencing;
        }
        self.raw_pending_id = self.raw_id;
        self.raw_id = self.raw_id.checked_add(1).ok_or(Error::InvalidResponse)?;
        self.raw_pending = Some(pending);
        Ok(())
    }
}
