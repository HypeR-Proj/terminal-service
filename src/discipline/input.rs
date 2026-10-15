// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::{Discipline, EventFlags, Record};

impl Discipline {
    /// Always inspect input for flow controls even when ordinary queues are
    /// full. Loss is explicit and cannot indefinitely hide a subsequent START.
    pub fn input(&mut self, bytes: &[u8], now_ns: u64) {
        for &original in bytes {
            let original = if self.settings.input.strip_high_bit {
                original & 0x7f
            } else {
                original
            };
            let original = if self.settings.input.lowercase {
                super::characters::lowercase(original)
            } else {
                original
            };
            if self.literal {
                self.literal = false;
                self.accept(original, true, now_ns);
                continue;
            }
            if self.settings.input.receive_flow_control {
                if Some(original) == self.settings.controls.stop {
                    self.received_stop = true;
                    continue;
                }
                if Some(original) == self.settings.controls.start {
                    if self.received_stop {
                        self.events.0 |= EventFlags::OUTPUT_RESUMED;
                    }
                    self.received_stop = false;
                    continue;
                }
                if self.settings.input.resume_on_any {
                    if self.received_stop {
                        self.events.0 |= EventFlags::OUTPUT_RESUMED;
                    }
                    self.received_stop = false;
                }
            }
            let byte = match original {
                b'\r' if self.settings.input.ignore_cr => continue,
                b'\r' if self.settings.input.cr_to_lf => b'\n',
                b'\n' if self.settings.input.lf_to_cr => b'\r',
                byte => byte,
            };
            if self.settings.canonical && self.control(byte) {
                continue;
            }
            // Outside canonical mode a literal LF is an ordinary control byte
            // for echo (^J). A CR converted to LF still echoes a line break.
            let quote_newline = !self.settings.canonical && original == b'\n';
            self.accept(byte, quote_newline, now_ns);
        }
        self.update_flow();
    }

    fn accept(&mut self, byte: u8, quoted: bool, now_ns: u64) {
        if self.settings.canonical {
            let delimiter =
                !quoted && (byte == b'\n' || Some(byte) == self.settings.controls.end_of_line);
            if self.discard_line {
                if delimiter {
                    self.discard_line = false;
                }
                return;
            }
            if self.edit.len() == self.limits.line_bytes
                || self.buffered_input() == self.limits.input_bytes
                || self.records.len() == self.limits.records
            {
                self.events.0 |= EventFlags::INPUT_LOSS;
                self.edit.clear();
                self.discard_line = !delimiter;
                return;
            }
            if self.edit.is_empty() {
                self.edit_start_column = self.column;
            }
            self.edit.push(byte);
            self.echo_input(byte, quoted);
            if delimiter {
                self.commit_line(false);
            }
        } else {
            if self.input.len() == self.limits.input_bytes {
                self.events.0 |= EventFlags::INPUT_LOSS;
                return;
            }
            if !self.records.back().is_some_and(|r| r.open) {
                if self.records.len() == self.limits.records {
                    self.events.0 |= EventFlags::INPUT_LOSS;
                    return;
                }
                self.records.push_back(Record {
                    remaining: 0,
                    open: true,
                });
            }
            self.input.push_back(byte);
            if let Some(record) = self.records.back_mut() {
                record.remaining += 1;
            }
            self.echo_input(byte, quoted);
        }
        self.last_input_ns = now_ns;
        self.input_generation = self.input_generation.wrapping_add(1);
    }

    pub(super) fn commit_line(&mut self, allow_empty: bool) {
        if self.edit.is_empty() && !allow_empty {
            return;
        }
        if self.records.len() == self.limits.records {
            self.events.0 |= EventFlags::INPUT_LOSS;
            self.edit.clear();
            return;
        }
        let remaining = self.edit.len();
        self.input.extend(self.edit.drain(..));
        self.records.push_back(Record {
            remaining,
            open: false,
        });
    }
}
