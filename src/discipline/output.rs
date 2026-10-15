// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::{Discipline, Settings};

/// Tracks a byte after output conversion. Output and echo update the column
/// only after the complete byte/sequence has been queued successfully.
pub(super) fn column_after_byte(column: usize, byte: u8, settings: Settings) -> usize {
    match byte {
        b'\r' => 0,
        b'\n' if settings.output.newline_resets_column => 0,
        b'\n' => column,
        b'\t' => (column / 8 + 1).saturating_mul(8),
        8 => column.saturating_sub(1),
        0..=31 | 127 => column,
        0x80..=0xbf if settings.utf8 => column,
        _ => column.saturating_add(1),
    }
}

impl Discipline {
    /// Returns accepted source bytes. Conversion is atomic per source byte, so
    /// a full queue can never split a generated CR/LF pair across settings.
    pub fn write_output(&mut self, bytes: &[u8]) -> usize {
        self.write_with_limit(
            bytes,
            self.limits.output_bytes - (self.limits.output_bytes / 2).min(256),
        )
    }

    /// Control sequences share normal output ordering, with reserved capacity
    /// so a departing foreground owner cannot prevent presentation recovery.
    pub fn write_control_output(&mut self, bytes: &[u8]) -> bool {
        if bytes.len() > self.output_capacity() {
            return false;
        }
        self.output.extend(bytes);
        true
    }

    pub(super) fn echo_limit(&self) -> usize {
        self.limits.output_bytes - (self.limits.output_bytes / 4).min(64)
    }

    pub(super) fn write_with_limit(&mut self, bytes: &[u8], limit: usize) -> usize {
        let mut accepted = 0;
        for &original in bytes {
            // Suppression applies to the original CR, before CR-to-LF.
            if original == b'\r'
                && self.settings.output.suppress_cr_at_column_zero
                && self.column == 0
            {
                accepted += 1;
                continue;
            }
            let pair = original == b'\n' && self.settings.output.lf_to_crlf;
            let byte = if original == b'\r' && self.settings.output.cr_to_lf {
                b'\n'
            } else if self.settings.output.uppercase {
                super::characters::uppercase(original)
            } else {
                original
            };
            let tab = byte == b'\t' && self.settings.output.expand_tabs;
            let needed = if tab {
                8 - self.column % 8
            } else if pair {
                2
            } else {
                1
            };
            if self.output.len() + needed > limit {
                break;
            }
            if tab {
                self.output.extend(core::iter::repeat_n(b' ', needed));
            } else {
                if pair {
                    self.output.push_back(b'\r');
                    self.column = 0;
                }
                self.output.push_back(byte);
            }
            self.column = column_after_byte(self.column, byte, self.settings);
            if original == b'\n'
                || (original == b'\r'
                    && (!self.settings.output.cr_to_lf
                        || self.settings.output.newline_resets_column))
            {
                self.edit_start_column = self.column;
            }
            accepted += 1;
        }
        accepted
    }

    pub fn output(&self, buffer: &mut [u8]) -> usize {
        if self.output_paused() {
            return 0;
        }
        let count = buffer.len().min(self.output.len());
        for (dst, src) in buffer.iter_mut().zip(self.output.iter()).take(count) {
            *dst = *src;
        }
        count
    }
    pub fn commit_output(&mut self, count: usize) {
        self.output.drain(..count.min(self.output.len()));
    }
    #[must_use]
    pub fn output_len(&self) -> usize {
        self.output.len()
    }
    #[must_use]
    pub fn output_capacity(&self) -> usize {
        self.limits.output_bytes - self.output.len()
    }
    pub fn flow_output(&self, buffer: &mut [u8]) -> usize {
        let count = buffer.len().min(self.flow.len());
        for (dst, src) in buffer.iter_mut().zip(self.flow.iter()).take(count) {
            *dst = *src;
        }
        count
    }
    pub fn commit_flow_output(&mut self, count: usize) {
        self.flow.drain(..count.min(self.flow.len()));
        self.update_flow();
    }
}
