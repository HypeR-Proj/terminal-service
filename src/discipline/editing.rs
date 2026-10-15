// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Canonical editing and echo. The reactor serializes input and output, so the
//! tracked column describes the tail of the output queue, including echoes.

use super::{Discipline, EventFlags};

impl Discipline {
    pub(super) fn control(&mut self, byte: u8) -> bool {
        let controls = self.settings.controls;
        if Some(byte) == controls.literal_next {
            self.literal = true;
            self.finish_erasing();
            if self.settings.echo && self.settings.echo_controls {
                self.echo_raw(b"^\x08");
            }
        } else if Some(byte) == controls.erase {
            self.erase(false);
        } else if Some(byte) == controls.kill {
            self.kill_line(byte);
        } else if Some(byte) == controls.word_erase {
            // N_TTY classifies the scalar's leading byte using its Latin-1
            // word class, not Unicode categories or whitespace alone.
            let mut in_word = false;
            while let Some(start) = self.erase_start() {
                let byte = self.edit[start];
                let word = super::characters::word(byte);
                if in_word && !word {
                    break;
                }
                in_word |= word;
                self.erase(true);
            }
        } else if Some(byte) == controls.reprint && self.settings.echo {
            self.finish_erasing();
            self.echo_character(byte);
            self.echo_transformed(b'\n');
            self.edit_start_column = self.column;
            for index in 0..self.edit.len() {
                self.echo_character(self.edit[index]);
            }
        } else if Some(byte) == controls.eof {
            self.finish_erasing();
            if !self.discard_line {
                self.commit_line(true);
            } else {
                self.discard_line = false;
                self.edit.clear();
            }
        } else {
            return false;
        }
        true
    }

    fn kill_line(&mut self, byte: u8) {
        self.discard_line = false;
        if self.edit.is_empty() {
            return;
        }
        if !self.settings.echo {
            self.edit.clear();
        } else if self.settings.echo_kill_line
            && self.settings.visual_kill
            && self.settings.visual_erase
        {
            while !self.edit.is_empty() {
                self.erase(true);
            }
        } else {
            self.edit.clear();
            self.finish_erasing();
            self.echo_character(byte);
            if self.settings.echo_kill_line {
                self.echo_transformed(b'\n');
            }
        }
    }

    /// Malformed UTF-8 removes one byte; valid scalars remain indivisible.
    fn erase_start(&self) -> Option<usize> {
        let end = self.edit.len();
        let mut start = end.checked_sub(1)?;
        if self.settings.utf8 {
            while start > 0 && end - start < 4 && self.edit[start] & 0xc0 == 0x80 {
                start -= 1;
            }
            if core::str::from_utf8(&self.edit[start..]).is_err() {
                start = end - 1;
            }
        }
        Some(start)
    }

    fn erase(&mut self, word_or_line: bool) {
        let Some(start) = self.erase_start() else {
            return;
        };
        let byte = self.edit[start];
        if self.settings.echo {
            if self.settings.echo_erased {
                if !self.erasing {
                    self.echo_raw(b"\\");
                    self.erasing = true;
                }
                for index in start..self.edit.len() {
                    self.echo_character(self.edit[index]);
                }
            } else if !word_or_line && !self.settings.visual_erase {
                if let Some(erase) = self.settings.controls.erase {
                    self.echo_character(erase);
                }
            } else if byte == b'\t' {
                // Tabs do not paint cells. Only move back to the tab's start;
                // do not overwrite the intervening cells with spaces.
                let width = self.tab_width(start).min(self.column);
                self.echo_raw(&[8; 8][..width]);
            } else {
                let width = if byte.is_ascii_control() {
                    usize::from(self.settings.echo_controls) * 2
                } else {
                    1
                };
                self.echo_raw(&b"\x08 \x08\x08 \x08"[..3 * width]);
            }
        }
        self.edit.truncate(start);
        if self.edit.is_empty() {
            self.finish_erasing();
        }
    }

    /// Only tabs require a scan. Ordinary erase and whole-line kill remain
    /// linear in the erased bytes rather than rescanning the line per scalar.
    fn tab_width(&self, end: usize) -> usize {
        let mut columns = 0;
        let mut origin = self.edit_start_column;
        for &byte in self.edit[..end].iter().rev() {
            if byte == b'\t' {
                origin = 0; // A previous tab ends on a tab stop.
                break;
            }
            columns += if byte.is_ascii_control() {
                usize::from(self.settings.echo_controls) * 2
            } else {
                usize::from(!self.settings.utf8 || byte & 0xc0 != 0x80)
            };
        }
        8 - (origin.saturating_add(columns) % 8)
    }

    pub(super) fn finish_erasing(&mut self) {
        if self.erasing {
            self.echo_raw(b"/");
            self.erasing = false;
        }
    }

    pub(super) fn echo_input(&mut self, byte: u8, quoted: bool) {
        let newline = byte == b'\n' && !quoted;
        if !self.settings.echo
            && !(self.settings.canonical && self.settings.echo_newline && newline)
        {
            return;
        }
        self.finish_erasing();
        if newline {
            self.echo_transformed(byte);
            self.edit_start_column = self.column;
        } else {
            self.echo_character(byte);
        }
    }

    fn echo_character(&mut self, byte: u8) {
        if self.settings.echo_controls && byte.is_ascii_control() && byte != b'\t' {
            self.echo_raw(&[b'^', if byte == 127 { b'?' } else { byte + 64 }]);
        } else {
            self.echo_transformed(byte);
        }
    }

    fn echo_transformed(&mut self, byte: u8) {
        if self.write_with_limit(&[byte], self.echo_limit()) == 0 {
            self.events.0 |= EventFlags::ECHO_LOSS;
        }
    }

    fn echo_raw(&mut self, bytes: &[u8]) {
        if bytes.len() > self.echo_limit().saturating_sub(self.output.len()) {
            self.events.0 |= EventFlags::ECHO_LOSS;
            return;
        }
        self.output.extend(bytes);
        for &byte in bytes {
            self.column = super::output::column_after_byte(self.column, byte, self.settings);
        }
    }
}
