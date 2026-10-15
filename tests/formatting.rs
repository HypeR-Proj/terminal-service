// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use crate::discipline::{CreateError, Discipline, Limits, ReadDecision, ReadRequest};
use hyper_terminal::{EventFlags, Settings};

fn hex(value: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    if value == "-" {
        return Ok(Vec::new());
    }
    (0..value.len())
        .step_by(2)
        .map(|i| Ok(u8::from_str_radix(&value[i..i + 2], 16)?))
        .collect()
}

fn settings(delta: &str) -> Result<Settings, Box<dyn std::error::Error>> {
    let mut settings = Settings::default();
    if delta != "-" {
        for field in delta.split(',') {
            let value = field.starts_with('+');
            match &field[1..] {
                "canonical" => settings.canonical = value,
                "echo" => settings.echo = value,
                "echo_controls" => settings.echo_controls = value,
                "echo_newline" => settings.echo_newline = value,
                "visual_erase" => settings.visual_erase = value,
                "echo_kill_line" => settings.echo_kill_line = value,
                "visual_kill" => settings.visual_kill = value,
                "echo_erased" => settings.echo_erased = value,
                "strip_high_bit" => settings.input.strip_high_bit = value,
                "lowercase" => settings.input.lowercase = value,
                "ignore_cr" => settings.input.ignore_cr = value,
                "lf_to_cr" => settings.input.lf_to_cr = value,
                "cr_to_lf" => settings.output.cr_to_lf = value,
                "expand_tabs" => settings.output.expand_tabs = value,
                "uppercase" => settings.output.uppercase = value,
                "suppress_cr_at_column_zero" => settings.output.suppress_cr_at_column_zero = value,
                _ => return Err(format!("unknown setting: {field}").into()),
            }
        }
    }
    Ok(settings)
}

/// The same byte fixtures can be checked against a real Linux PTY using
/// linux-formatting.py. No Linux ABI or implementation enters the service.
#[test]
fn linux_formatting_vectors() -> Result<(), Box<dyn std::error::Error>> {
    for row in include_str!("formatting.tsv")
        .lines()
        .filter(|l| !l.starts_with('#'))
    {
        let fields: Vec<_> = row.split('\t').collect();
        assert_eq!(fields.len(), 6);
        let [name, direction, delta, source, data, output] = fields[..] else {
            return Err("invalid fixture".into());
        };
        let settings = settings(delta)?;
        let source = hex(source)?;
        let data = hex(data)?;
        let output = hex(output)?;
        // A transport can split at any byte, including UTF-8 and quote pairs.
        for split in 0..=source.len() {
            let mut terminal = Discipline::new(settings, Limits::default())
                .map_err(|error| format!("{error:?}"))?;
            if direction == "input" {
                terminal.input(&source[..split], 1);
                terminal.input(&source[split..], 2);
                let mut bytes = [0; 256];
                let decision = ReadRequest::new(settings, bytes.len(), 3, u64::MAX)
                    .poll(&terminal, 3, &mut bytes);
                assert_eq!(decision, ReadDecision::Data(data.len()), "{name}");
                assert_eq!(&bytes[..data.len()], data, "{name}");
            } else {
                assert_eq!(terminal.write_output(&source[..split]), split, "{name}");
                assert_eq!(
                    terminal.write_output(&source[split..]),
                    source.len() - split,
                    "{name}"
                );
            }
            let mut bytes = [0; 256];
            let count = terminal.output(&mut bytes);
            assert_eq!(&bytes[..count], output, "{name}; split={split}");
            assert_eq!(terminal.take_events().0, 0, "{name}");
        }
    }
    Ok(())
}

#[test]
fn canonical_throttle_waits_for_a_readable_record() -> Result<(), CreateError> {
    let mut settings = Settings::default();
    settings.input.send_flow_control = true;
    let limits = Limits {
        input_bytes: 32,
        line_bytes: 16,
        high_water: 8,
        low_water: 4,
        ..Limits::default()
    };
    let mut terminal = Discipline::new(settings, limits)?;
    terminal.input(b"12345678", 1);
    assert!(!terminal.peer_stopped());
    terminal.input(b"\nabcdefgh", 2);
    assert!(terminal.peer_stopped());
    terminal.commit_read(ReadDecision::Data(9));
    // The remaining partial line cannot be read; allow its delimiter through.
    assert!(!terminal.peer_stopped());
    Ok(())
}

#[test]
fn dropped_echo_does_not_move_the_tracked_column() -> Result<(), CreateError> {
    let mut settings = Settings::default();
    settings.output.expand_tabs = true;
    let mut terminal = Discipline::new(
        settings,
        Limits {
            output_bytes: 16,
            ..Limits::default()
        },
    )?;
    terminal.input(b"abcdefghijk\x01", 1); // 11 bytes fit; the two-byte ^A does not.
    assert_eq!(terminal.output_len(), 11);
    assert_ne!(terminal.take_events().0 & EventFlags::ECHO_LOSS, 0);
    terminal.commit_output(11);
    assert_eq!(terminal.write_output(b"\t"), 1);
    let mut bytes = [0; 8];
    assert_eq!(terminal.output(&mut bytes), 5);
    assert_eq!(&bytes[..5], b"     ");
    Ok(())
}

#[test]
fn empty_read_keeps_the_next_eof_record() -> Result<(), CreateError> {
    let settings = Settings::default();
    let mut terminal = Discipline::new(settings, Limits::default())?;
    terminal.input(b"\x04", 1);
    let decision = ReadRequest::new(settings, 0, 2, u64::MAX).poll(&terminal, 2, &mut []);
    assert_eq!(decision, ReadDecision::Data(0));
    terminal.commit_read(decision);
    assert_eq!(
        ReadRequest::new(settings, 1, 3, u64::MAX).poll(&terminal, 3, &mut [0]),
        ReadDecision::EndOfInput
    );
    Ok(())
}
