// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl Reactor {
    pub(super) fn prepare(&mut self, parent: u64, job: bool) -> Result<(u64, PreparedHandles)> {
        if self.leases.len() == MAX_LEASES {
            return Err(Error::Status(Status::RESOURCE_LIMIT));
        }
        let id = self.next_lease;
        self.next_lease = id
            .checked_add(1)
            .ok_or(Error::Status(Status::RESOURCE_LIMIT))?;
        let (stream_server, stream_client) = CapabilityChannel::create()?;
        let (control_server, control_client) = CapabilityChannel::create()?;
        let (events, event_client) = hyper_os::channel::create_pair()?;
        let (lifetime, supervisor) = if job {
            let (server, client) = hyper_os::channel::create_pair()?;
            (
                LeaseLifetime::Job {
                    supervisor: server,
                    presentation_owner: None,
                    restore_presentation: false,
                    pending: false,
                    next_sequence: 1,
                },
                Some(client),
            )
        } else {
            (LeaseLifetime::Unbound, None)
        };
        let alive = Arc::new(AtomicBool::new(true));
        admission::stream_worker(id, stream_server, self.publisher.clone(), alive.clone())?;
        if let Err(error) =
            admission::control_worker(id, control_server, self.publisher.clone(), alive.clone())
        {
            alive.store(false, Ordering::Release);
            return Err(error);
        }
        self.leases.push(Lease {
            id,
            parent,
            alive,
            lifetime,
            activated: false,
            startup_output_remaining: stream::MAX_DATA,
            settings: Settings::default(),
            events,
            event_flags: 0,
            presentation: 0,
        });
        Ok((
            id,
            PreparedHandles {
                stream: Some(stream_client.into_handle()),
                control: Some(control_client.into_handle()),
                events: Some(event_client),
                supervisor,
            },
        ))
    }

    pub(super) fn retire_owners(&mut self, _now: u64) -> Result<()> {
        loop {
            let dead = self
                .leases
                .iter()
                .find(|lease| match &lease.lifetime {
                    LeaseLifetime::Process(owner) => {
                        owner.as_process_supervisor().wait_terminated(0).is_ok()
                    }
                    LeaseLifetime::Job { supervisor, .. } => wait_many(
                        &[WaitItem::new(
                            supervisor.as_handle_ref(),
                            ObjectSignals::<ByteChannelObject>::PEER_CLOSED,
                        )],
                        0,
                    )
                    .is_ok(),
                    LeaseLifetime::Unbound => false,
                })
                .map(|lease| lease.id);
            match dead {
                Some(id) => self.revoke(id)?,
                None => break,
            }
        }
        Ok(())
    }

    pub(super) fn revoke(&mut self, id: u64) -> Result<()> {
        let Some(root) = self.leases.iter().find(|lease| lease.id == id) else {
            return Ok(());
        };
        let parent = root.parent;
        // Remove a subtree as one foreground transition. Recursive restoration
        // could start an asynchronous flow-profile change to a parent that the
        // outer revoke then removes, leaving input assigned to a dead lease.
        let mut retired = [0; MAX_LEASES];
        retired[0] = id;
        let mut count = 1;
        let mut cursor = 0;
        while cursor < count {
            let parent = retired[cursor];
            for child in self.leases.iter().filter(|lease| lease.parent == parent) {
                *retired.get_mut(count).ok_or(Error::InvalidResponse)? = child.id;
                count += 1;
            }
            cursor += 1;
        }
        let retired = &retired[..count];
        if self.configuration.as_ref().is_some_and(|config| {
            retired.contains(&config.target)
                || config
                    .request
                    .as_ref()
                    .is_some_and(|request| retired.contains(&request.lease))
        }) || self
            .input_flush
            .as_ref()
            .is_some_and(|flush| retired.contains(&flush.request.lease))
        {
            // The manager has the last acknowledged recovery profile. Restart
            // the session rather than guessing which peer-control byte applied.
            return Err(Error::Status(Status::CANCELLED));
        }
        let mut reset_presentation = false;
        self.leases.retain(|lease| {
            if retired.contains(&lease.id) {
                lease.alive.store(false, Ordering::Release);
                reset_presentation |= lease.presentation != 0;
                false
            } else {
                true
            }
        });
        self.connections
            .retain(|connection| !retired.contains(&connection.lease));
        if retired.contains(&self.foreground) {
            self.terminal.resume_output();
            let settings = self
                .leases
                .iter()
                .find(|lease| lease.id == parent)
                .map_or(Settings::default(), |lease| lease.settings);
            self.foreground = 0;
            self.begin_configuration(None, parent, settings, reset_presentation)?;
        }
        Ok(())
    }

    pub(super) fn cancel_reads_except(&mut self, except: u64, reason: u16) {
        for connection in &mut self.connections {
            if connection.lease != except
                && matches!(connection.pending, Some(Pending::Read { .. }))
                && let Some(Pending::Read { header, .. }) = connection.pending.take()
            {
                connection.response =
                    Some(StreamHeader::response(stream::READ, header.id, reason, 0, 0).encode());
            }
        }
    }

    pub(super) fn notify(&mut self, flags: u32) {
        for lease in &mut self.leases {
            lease.event_flags |= flags;
            let _ = lease
                .events
                .as_byte_channel()
                .try_send(&flags.to_le_bytes());
        }
    }

    pub(super) fn publish_terminal_events(&mut self) {
        let flags = self.terminal.take_events().0;
        if flags & EventFlags::TERMINATE != 0 {
            self.request_termination();
        }
        if flags & !EventFlags::TERMINATE != 0 {
            self.notify(flags & !EventFlags::TERMINATE);
        }
    }

    pub(super) fn request_termination(&mut self) {
        let Some(lease) = self
            .leases
            .iter_mut()
            .find(|lease| lease.id == self.foreground)
        else {
            return;
        };
        match &mut lease.lifetime {
            LeaseLifetime::Job { pending, .. } => *pending = true,
            // A process-owned lease (including the interactive shell) receives
            // only its own request. Background parents never inherit this bit.
            _ => {
                lease.event_flags |= EventFlags::TERMINATE;
                let _ = lease
                    .events
                    .as_byte_channel()
                    .try_send(&EventFlags::TERMINATE.to_le_bytes());
            }
        }
    }

    pub(super) fn deliver_termination(&mut self) -> Result<()> {
        for lease in &mut self.leases {
            let LeaseLifetime::Job {
                supervisor,
                pending,
                next_sequence,
                ..
            } = &mut lease.lifetime
            else {
                continue;
            };
            if !*pending {
                continue;
            }
            let next = next_sequence.checked_add(1).ok_or(Error::InvalidResponse)?;
            let event = hyper_terminal::TerminationEvent {
                lease: lease.id,
                sequence: *next_sequence,
            };
            match supervisor.as_byte_channel().try_send(&event.encode()) {
                Ok(()) => {
                    *pending = false;
                    *next_sequence = next;
                }
                Err(Error::Status(Status::WOULD_BLOCK | Status::PEER_CLOSED)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub(super) fn recover_job_presentations(&mut self) -> Result<()> {
        let mut restore = None;
        for lease in &mut self.leases {
            let LeaseLifetime::Job {
                presentation_owner,
                restore_presentation,
                ..
            } = &mut lease.lifetime
            else {
                continue;
            };
            if presentation_owner
                .as_ref()
                .is_some_and(|owner| owner.as_process_supervisor().wait_terminated(0).is_ok())
            {
                // Stop watching the completed Process even while another
                // configuration transaction delays restoration.
                presentation_owner.take();
                *restore_presentation = true;
                lease.settings = Settings::default();
            }
            if *restore_presentation
                && self.foreground == lease.id
                && self.configuration.is_none()
                && self.input_flush.is_none()
            {
                *restore_presentation = false;
                let reset_display = lease.presentation != 0;
                lease.presentation = 0;
                restore = Some((lease.id, reset_display));
            }
        }
        if let Some((id, reset_display)) = restore {
            self.terminal.resume_output();
            // This preserves STOP/START fencing and recovery-profile ownership.
            // Resetting the discipline directly would bypass that transaction.
            // Ordinary commands neither change the display nor need a redundant
            // profile transition that would cancel surviving members' reads.
            if reset_display || self.terminal.settings() != Settings::default() {
                self.begin_configuration(None, id, Settings::default(), reset_display)?;
            }
        }
        Ok(())
    }
}
