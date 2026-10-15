// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Traditional terminal byte classes. Case conversion follows `N_TTY`'s Latin-1
//! mapping, independently of UTF-8 erase grouping. It is disabled by default.

const fn upper(byte: u8) -> bool {
    matches!(byte, b'A'..=b'Z' | 0xc0..=0xd6 | 0xd8..=0xde)
}

const fn lower(byte: u8) -> bool {
    matches!(byte, b'a'..=b'z' | 0xdf..=0xf6 | 0xf8..=0xff)
}

pub(super) const fn word(byte: u8) -> bool {
    upper(byte) || lower(byte) || byte.is_ascii_digit() || byte == b'_'
}

pub(super) const fn lowercase(byte: u8) -> u8 {
    if upper(byte) { byte + 32 } else { byte }
}

pub(super) const fn uppercase(byte: u8) -> u8 {
    if lower(byte) { byte - 32 } else { byte }
}
