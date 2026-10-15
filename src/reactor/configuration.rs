// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl Reactor {
    pub(super) fn configure_progress(&mut self, now: u64) -> Result<()> {
        let Some(config) = self.configuration.as_mut() else {
            return Ok(());
        };
        if now >= config.deadline {
            return Err(Error::Status(Status::TIMED_OUT));
        }
        let mut profile = [0; 16];
        profile[..8].copy_from_slice(&config.generation.to_le_bytes());
        let start = if config.settings.input.send_flow_control {
            config.settings.controls.start.map_or(256, u16::from)
        } else {
            256
        };
        profile[8..10].copy_from_slice(&start.to_le_bytes());
        if config.phase == ConfigurationPhase::NotifyManager {
            match self.recovery.as_byte_channel().try_send(&profile) {
                Ok(()) => {}
                Err(Error::Status(Status::WOULD_BLOCK)) => return Ok(()),
                Err(error) => return Err(error),
            }
            config.phase = ConfigurationPhase::AwaitManager;
        }
        if config.phase == ConfigurationPhase::AwaitManager {
            let mut response = [0; 16];
            match self.recovery.as_byte_channel().try_receive(&mut response) {
                Ok(16) if response == profile => {}
                Err(Error::Status(Status::WOULD_BLOCK)) => return Ok(()),
                _ => return Err(Error::InvalidResponse),
            }
            let Some(config) = self.configuration.take() else {
                return Err(Error::InvalidResponse);
            };
            self.apply_configuration(
                config.request,
                config.target,
                config.settings,
                config.reset_presentation,
            )?;
        }
        Ok(())
    }

    pub(super) fn begin_configuration(
        &mut self,
        request: Option<ControlRequest>,
        target: u64,
        settings: Settings,
        reset_presentation: bool,
    ) -> Result<()> {
        let old = self.terminal.settings();
        if self.configuration.is_some() {
            return Err(Error::Status(Status::BUSY));
        }
        if !old.input.send_flow_control
            && !settings.input.send_flow_control
            && !self.terminal.peer_stopped()
        {
            return self.apply_configuration(request, target, settings, reset_presentation);
        }
        self.terminal.suspend_send_flow(true);
        self.terminal.resume_peer();
        self.recovery_generation = self
            .recovery_generation
            .checked_add(1)
            .ok_or(Error::InvalidResponse)?;
        self.configuration = Some(Configuration {
            request,
            target,
            settings,
            reset_presentation,
            generation: self.recovery_generation,
            phase: ConfigurationPhase::ResumePeer,
            deadline: hyper_os::time::monotonic_now()?
                .as_nanoseconds()
                .saturating_add(5_000_000_000),
        });
        Ok(())
    }

    pub(super) fn apply_configuration(
        &mut self,
        request: Option<ControlRequest>,
        target: u64,
        settings: Settings,
        reset_presentation: bool,
    ) -> Result<()> {
        let foreground_changed = self.foreground != target;
        if foreground_changed {
            self.cancel_reads_except(target, stream::CONFIGURATION_CHANGED);
        } else {
            self.cancel_reads_except(u64::MAX, stream::CONFIGURATION_CHANGED);
        }
        self.terminal
            .configure(settings)
            .map_err(|_| Error::InvalidResponse)?;
        self.terminal.suspend_send_flow(false);
        self.foreground = target;
        if let Some(lease) = self.leases.iter_mut().find(|lease| lease.id == target) {
            lease.settings = settings;
            lease.activated = true;
        }
        if reset_presentation {
            self.terminal.set_output_paused(false);
            // The discipline reserves capacity for one complete baseline reset;
            // it follows all accepted output from the departing foreground.
            let reset = b"\x1b[0m\x1b[?25h\x1b[?1049l";
            if self.terminal.output_capacity() < reset.len() {
                return Err(Error::Status(Status::RESOURCE_LIMIT));
            }
            self.terminal.write_control_output(reset);
        }
        if foreground_changed {
            self.notify(EventFlags::FOREGROUND_CHANGED);
        }
        if let Some(request) = request {
            request.respond(Status::OK, 0, None, None);
        }
        Ok(())
    }

    pub(super) fn finish_input_flush(&mut self) -> Result<()> {
        let Some(flush) = self.input_flush.as_mut() else {
            return Ok(());
        };
        if flush.phase == InputFlushPhase::SendDiscard {
            match self
                .raw_control
                .as_byte_channel()
                .try_send(&StreamHeader::request(stream::DISCARD_INPUT, flush.id, 0, 0).encode())
            {
                Ok(()) => flush.phase = InputFlushPhase::AwaitDiscard,
                Err(Error::Status(Status::WOULD_BLOCK)) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
        if flush.phase != InputFlushPhase::AwaitDiscard {
            return Ok(());
        }
        let mut bytes = [0; stream::HEADER_BYTES];
        let count = match self.raw_control.as_byte_channel().try_receive(&mut bytes) {
            Ok(count) => count,
            Err(Error::Status(Status::WOULD_BLOCK)) => return Ok(()),
            Err(error) => return Err(error),
        };
        let header = StreamHeader::decode(&bytes[..count])?;
        if count != stream::HEADER_BYTES
            || header.operation != stream::DISCARD_INPUT | stream::RESPONSE
            || header.id != flush.id
            || header.flags != 0
        {
            return Err(Error::InvalidResponse);
        }
        header.status()?;
        self.terminal.clear_input();
        self.ingress.clear();
        if let Some(flush) = self.input_flush.take() {
            flush.request.respond(Status::OK, 0, None, None);
        }
        Ok(())
    }
}
