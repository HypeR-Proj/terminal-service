// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl Reactor {
    pub(super) fn control(&mut self, mut request: ControlRequest) -> Result<()> {
        let operation = request.header.operation;
        let lease_id = request.lease;
        let managed = lease_id == 0;
        let active = managed || self.foreground == lease_id;
        if !managed && !self.leases.iter().any(|lease| lease.id == lease_id) {
            request.respond(Status::CANCELLED, 0, None, None);
            return Ok(());
        }
        if !managed
            && self
                .leases
                .iter()
                .any(|lease| lease.id == lease_id && !lease.activated)
        {
            if self.deferred.len() < MAX_LEASES {
                self.deferred.push(request);
            } else {
                request.respond(Status::RESOURCE_LIMIT, 0, None, None);
            }
            return Ok(());
        }
        match operation {
            protocol::PREPARE if active => match self.prepare(lease_id) {
                Ok((id, handles)) => request.respond(Status::OK, id, None, Some(handles)),
                Err(Error::Status(status)) => request.respond(status, 0, None, None),
                Err(_) => request.respond(Status::INTERNAL, 0, None, None),
            },
            protocol::BIND if active => {
                if self.configuration.is_some() || self.input_flush.is_some() {
                    request.respond(Status::BUSY, 0, None, None);
                    return Ok(());
                }
                let id = request.header.value;
                let index = self.leases.iter().position(|lease| {
                    lease.id == id && lease.parent == lease_id && lease.owner.is_none()
                });
                if let (Some(index), Some(owner)) = (index, request.observer.take()) {
                    let terminated = owner.as_process_supervisor().wait_terminated(0).is_ok();
                    if terminated {
                        // A non-reading command can finish between launch
                        // readiness and this bind. Its supervisor reports its
                        // exit status; no foreground input was ever granted.
                        self.revoke(id)?;
                        request.respond(Status::OK, 0, None, None);
                    } else {
                        self.leases[index].owner = Some(owner);
                        let settings = self.leases[index].settings;
                        self.begin_configuration(Some(request), id, settings, false)?;
                    }
                } else {
                    request.respond(Status::BAD_STATE, 0, None, None);
                }
            }
            protocol::RELEASE => {
                let id = request.header.value;
                if self
                    .leases
                    .iter()
                    .any(|lease| lease.id == id && (managed || lease.parent == lease_id))
                {
                    self.revoke(id)?;
                    request.respond(Status::OK, 0, None, None);
                } else if id < self.next_lease && !self.leases.iter().any(|lease| lease.id == id) {
                    request.respond(Status::OK, 0, None, None);
                } else {
                    request.respond(Status::ACCESS_DENIED, 0, None, None);
                }
            }
            protocol::GET_SETTINGS => {
                let settings = self
                    .leases
                    .iter()
                    .find(|lease| lease.id == lease_id)
                    .map_or(Settings::default(), |lease| lease.settings);
                request.respond(
                    Status::OK,
                    0,
                    Some(protocol::encode_settings(settings)),
                    None,
                );
            }
            protocol::SET_SETTINGS if active => {
                if let Some(settings) = protocol::decode_settings(&request.payload) {
                    if self.configuration.is_some() || self.input_flush.is_some() {
                        request.respond(Status::BUSY, 0, None, None);
                    } else {
                        self.begin_configuration(Some(request), lease_id, settings, false)?;
                    }
                } else {
                    request.respond(Status::INVALID_ARGUMENT, 0, None, None);
                }
            }
            protocol::GET_SIZE => request.respond(
                Status::OK,
                u64::from(self.columns) | u64::from(self.rows) << 16,
                None,
                None,
            ),
            protocol::SET_SIZE if active => {
                let value = request.header.value;
                if value >> 32 != 0 || value as u16 == 0 || (value >> 16) as u16 == 0 {
                    request.respond(Status::INVALID_ARGUMENT, 0, None, None);
                } else {
                    self.columns = value as u16;
                    self.rows = (value >> 16) as u16;
                    self.notify(EventFlags::SIZE_CHANGED);
                    request.respond(Status::OK, 0, None, None);
                }
            }
            protocol::TAKE_EVENTS => {
                let events = self
                    .leases
                    .iter_mut()
                    .find(|lease| lease.id == lease_id)
                    .map_or(0, |lease| std::mem::take(&mut lease.event_flags));
                request.respond(Status::OK, u64::from(events), None, None);
            }
            protocol::CLEAR_INPUT if active => {
                if self.input_flush.is_some() || self.configuration.is_some() {
                    request.respond(Status::BUSY, 0, None, None);
                } else {
                    self.cancel_reads_except(u64::MAX, stream::CANCELLED);
                    if self.raw_read_capacity != 0 && !self.raw_cancel_sent {
                        self.raw_input.cancel()?;
                        self.raw_cancel_sent = true;
                    }
                    let id = self.raw_id;
                    self.raw_id = self.raw_id.checked_add(1).ok_or(Error::InvalidResponse)?;
                    self.input_flush = Some(InputFlush {
                        request,
                        phase: if self.raw_read_capacity == 0 {
                            InputFlushPhase::SendDiscard
                        } else {
                            InputFlushPhase::Cancelling
                        },
                        id,
                    });
                }
            }
            protocol::PAUSE_OUTPUT if active && request.header.value <= 1 => {
                self.terminal.set_output_paused(request.header.value != 0);
                request.respond(Status::OK, 0, None, None);
            }
            protocol::PRESENTATION if active && request.header.value <= 3 => {
                let flags = request.header.value;
                let sequence: &[u8] = match flags {
                    0 => b"\x1b[?25h\x1b[?1049l",
                    1 => b"\x1b[?1049h\x1b[?25h",
                    2 => b"\x1b[?1049l\x1b[?25l",
                    _ => b"\x1b[?1049h\x1b[?25l",
                };
                if self.terminal.write_control_output(sequence) {
                    if let Some(lease) = self.leases.iter_mut().find(|lease| lease.id == lease_id) {
                        lease.presentation = flags;
                    }
                    request.respond(Status::OK, 0, None, None);
                } else {
                    request.respond(Status::BUSY, 0, None, None);
                }
            }
            _ => request.respond(Status::ACCESS_DENIED, 0, None, None),
        }
        Ok(())
    }
}
