// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use crate::discipline::{CreateError, Discipline, Limits, ReadDecision, ReadRequest};
use hyper_terminal::{EventFlags, ReadPolicy, Settings};

fn terminal(settings: Settings) -> Result<Discipline, CreateError> {
    Discipline::new(settings, Limits::default())
}

fn read(terminal: &mut Discipline, capacity: usize, now: u64) -> (ReadDecision, Vec<u8>) {
    let mut bytes = vec![0; capacity];
    let mut request = ReadRequest::new(terminal.settings(), capacity, now, u64::MAX);
    let decision = request.poll(terminal, now, &mut bytes);
    if let ReadDecision::Data(count) = decision {
        bytes.truncate(count);
    } else {
        bytes.clear();
    }
    terminal.commit_read(decision);
    (decision, bytes)
}

#[test]
fn canonical_eof_commits_partial_line_without_extra_eof() -> Result<(), CreateError> {
    let mut terminal = terminal(Settings::default())?;
    terminal.input(b"abc\x04", 1);
    assert_eq!(
        read(&mut terminal, 2, 2),
        (ReadDecision::Data(2), b"ab".to_vec())
    );
    assert_eq!(
        read(&mut terminal, 9, 3),
        (ReadDecision::Data(1), b"c".to_vec())
    );
    assert_eq!(read(&mut terminal, 9, 4).0, ReadDecision::Pending);
    terminal.input(b"\x04x\n", 5);
    assert_eq!(read(&mut terminal, 9, 6).0, ReadDecision::EndOfInput);
    assert_eq!(read(&mut terminal, 9, 7).1, b"x\n");
    Ok(())
}

#[test]
fn raw_preserves_every_byte_including_flow_and_eof() -> Result<(), CreateError> {
    let mut terminal = terminal(Settings::raw())?;
    let bytes: Vec<u8> = (0..=255).collect();
    terminal.input(&bytes, 1);
    assert_eq!(read(&mut terminal, 256, 2).1, bytes);
    assert_eq!(terminal.write_output(&bytes), 256);
    let mut output = [0; 256];
    assert_eq!(terminal.output(&mut output), 256);
    assert_eq!(output.as_slice(), bytes);
    Ok(())
}

#[test]
fn literal_next_bypasses_conversion_delimiter_and_flow() -> Result<(), CreateError> {
    let mut terminal = terminal(Settings::default())?;
    terminal.input(b"\x16\r\x16\n\x16\x13\n", 1);
    assert!(!terminal.output_paused());
    assert_eq!(read(&mut terminal, 32, 2).1, b"\r\n\x13\n");
    Ok(())
}

#[test]
fn editing_respects_utf8_scalar_and_word_boundaries() -> Result<(), CreateError> {
    let mut terminal = terminal(Settings::default())?;
    terminal.input("abé".as_bytes(), 1);
    terminal.input(b"\x7f cd\x17X\n", 2);
    assert_eq!(read(&mut terminal, 32, 3).1, b"ab X\n");
    Ok(())
}

#[test]
fn visual_erase_removes_both_columns_of_a_quoted_carriage_return() -> Result<(), CreateError> {
    let mut terminal = terminal(Settings::default())?;
    terminal.input(b"a\x16\r\x7f\n", 1);
    let mut output = [0; 32];
    let count = terminal.output(&mut output);
    assert_eq!(&output[..count], b"a^\x08^M\x08 \x08\x08 \x08\r\n");
    assert_eq!(read(&mut terminal, 32, 2).1, b"a\n");
    Ok(())
}

#[test]
fn byte_mode_erase_keeps_following_tab_expansion_aligned() -> Result<(), CreateError> {
    let mut settings = Settings {
        utf8: false,
        ..Settings::default()
    };
    settings.output.expand_tabs = true;
    let mut terminal = terminal(settings)?;
    terminal.input(b"a\x80\x7f\t\n", 1);
    let mut output = [0; 32];
    let count = terminal.output(&mut output);
    assert_eq!(&output[..count], b"a\x80\x08 \x08       \r\n");
    assert_eq!(read(&mut terminal, 32, 2).1, b"a\t\n");
    Ok(())
}

#[test]
fn overflow_drops_entire_unfinished_line_and_reports_loss() -> Result<(), CreateError> {
    let limits = Limits {
        line_bytes: 4,
        ..Limits::default()
    };
    let mut terminal = Discipline::new(Settings::default(), limits)?;
    terminal.input(b"12345\nOK\n", 1);
    assert_eq!(
        terminal.take_events().0 & EventFlags::INPUT_LOSS,
        EventFlags::INPUT_LOSS
    );
    assert_eq!(read(&mut terminal, 32, 2).1, b"OK\n");
    Ok(())
}

#[test]
fn settings_changes_do_not_reinterpret_buffered_input() -> Result<(), CreateError> {
    let mut terminal = terminal(Settings::default())?;
    terminal.input(b"part", 1);
    assert!(terminal.configure(Settings::raw()).is_ok());
    assert_eq!(read(&mut terminal, 8, 2).1, b"part");
    terminal.input(b"\x04\r", 3);
    assert!(terminal.configure(Settings::default()).is_ok());
    assert_eq!(read(&mut terminal, 8, 4).1, b"\x04\r");
    Ok(())
}

#[test]
fn cooked_to_binary_read_does_not_wait_for_new_bytes_in_the_old_record() -> Result<(), CreateError>
{
    let mut cooked = Settings::raw();
    cooked.input.receive_flow_control = true;
    let mut terminal = terminal(cooked)?;
    terminal.input(b"ab", 1);
    let mut binary = Settings::raw();
    binary.read_policy = ReadPolicy::AtLeast { minimum: 4 };
    assert!(terminal.configure(binary).is_ok());
    // The service retains subsequent binary bytes in untouched ingress, so
    // the previously cooked tail can never grow to meet the new minimum.
    assert_eq!(
        read(&mut terminal, 8, 2),
        (ReadDecision::Data(2), b"ab".to_vec())
    );
    terminal.input(b"cd", 3);
    assert_eq!(read(&mut terminal, 8, 4).0, ReadDecision::Pending);
    terminal.input(b"ef", 5);
    assert_eq!(read(&mut terminal, 8, 6).1, b"cdef");
    Ok(())
}

#[test]
fn noncanonical_policies_have_distinct_empty_and_timeout_results() -> Result<(), CreateError> {
    let mut settings = Settings::raw();
    settings.read_policy = ReadPolicy::Available;
    let terminal = terminal(settings)?;
    let mut bytes = [0; 8];
    assert_eq!(
        ReadRequest::new(settings, 8, 1, u64::MAX).poll(&terminal, 1, &mut bytes),
        ReadDecision::WouldBlock
    );
    settings.read_policy = ReadPolicy::FirstByteTimeout { timeout_ns: 10 };
    let mut request = ReadRequest::new(settings, 8, 1, u64::MAX);
    assert_eq!(
        request.poll(&terminal, 10, &mut bytes),
        ReadDecision::Pending
    );
    assert_eq!(
        request.poll(&terminal, 11, &mut bytes),
        ReadDecision::TimedOut
    );
    Ok(())
}

#[test]
fn interbyte_timer_waits_for_first_byte_then_restarts_on_progress() -> Result<(), CreateError> {
    let mut settings = Settings::raw();
    settings.read_policy = ReadPolicy::InterByteTimeout {
        minimum: 4,
        gap_ns: 10,
    };
    let mut terminal = terminal(settings)?;
    let mut request = ReadRequest::new(settings, 8, 1, u64::MAX);
    let mut bytes = [0; 8];
    assert_eq!(
        request.poll(&terminal, 100, &mut bytes),
        ReadDecision::Pending
    );
    terminal.input(b"a", 101);
    assert_eq!(
        request.poll(&terminal, 105, &mut bytes),
        ReadDecision::Pending
    );
    terminal.input(b"b", 110);
    assert_eq!(
        request.poll(&terminal, 119, &mut bytes),
        ReadDecision::Pending
    );
    assert_eq!(
        request.poll(&terminal, 120, &mut bytes),
        ReadDecision::Data(2)
    );
    // Polling did not consume bytes; a cancelled/unpublished result leaves them.
    assert_eq!(terminal.buffered_input(), 2);
    terminal.commit_read(ReadDecision::Data(2));
    assert_eq!(terminal.buffered_input(), 0);
    Ok(())
}

#[test]
fn flow_direction_is_independent_and_priority_resume_survives_output_pause()
-> Result<(), CreateError> {
    let mut settings = Settings::raw();
    settings.input.receive_flow_control = true;
    settings.input.send_flow_control = true;
    let limits = Limits {
        input_bytes: 16,
        line_bytes: 4,
        low_water: 3,
        high_water: 8,
        ..Limits::default()
    };
    let mut terminal = Discipline::new(settings, limits)?;
    terminal.input(b"\x13abcdefgh", 1);
    assert!(terminal.output_paused());
    let mut flow = [0; 8];
    assert_eq!(terminal.flow_output(&mut flow), 1);
    assert_eq!(flow[0], 19);
    terminal.commit_flow_output(1);
    assert_eq!(read(&mut terminal, 8, 2).1, b"abcdefgh");
    assert_eq!(terminal.flow_output(&mut flow), 1);
    assert_eq!(flow[0], 17);
    terminal.set_output_paused(true);
    terminal.input(b"\x11", 3);
    assert!(terminal.output_paused());
    terminal.set_output_paused(false);
    assert!(!terminal.output_paused());
    Ok(())
}

#[test]
fn minimum_is_capped_by_request_and_sealed_canonical_record() -> Result<(), CreateError> {
    let mut settings = Settings::raw();
    settings.read_policy = ReadPolicy::AtLeast { minimum: 100 };
    let mut terminal = terminal(settings)?;
    terminal.input(b"abc", 1);
    assert_eq!(read(&mut terminal, 3, 2).1, b"abc");
    assert!(terminal.configure(Settings::default()).is_ok());
    terminal.input(b"x\n", 3);
    assert!(terminal.configure(settings).is_ok());
    assert_eq!(read(&mut terminal, 32, 4).1, b"x\n");
    Ok(())
}

#[test]
fn newline_only_echo_and_output_transformations_are_bounded() -> Result<(), CreateError> {
    let mut settings = Settings {
        echo: false,
        echo_newline: true,
        ..Settings::default()
    };
    settings.input.lowercase = true;
    settings.output.expand_tabs = true;
    settings.output.suppress_cr_at_column_zero = true;
    let mut terminal = terminal(settings)?;
    terminal.input(b"ABC\n", 1);
    assert_eq!(read(&mut terminal, 10, 2).1, b"abc\n");
    let mut output = [0; 32];
    assert_eq!(terminal.output(&mut output), 2);
    assert_eq!(&output[..2], b"\r\n");
    terminal.commit_output(2);
    assert_eq!(terminal.write_output(b"\ra\tb\n"), 5);
    let count = terminal.output(&mut output);
    assert_eq!(&output[..count], b"a       b\r\n");
    Ok(())
}

#[test]
fn full_output_still_accepts_flow_control_and_preserves_recovery_reserve() -> Result<(), CreateError>
{
    let mut terminal = terminal(Settings::default())?;
    terminal.input(b"\x13", 1);
    let bytes = vec![b'x'; 65536];
    assert!(terminal.write_output(&bytes) < bytes.len());
    terminal.input(&bytes[..4096], 2);
    assert!(terminal.take_events().0 & EventFlags::ECHO_LOSS != 0);
    terminal.input(b"\x11", 3);
    assert!(!terminal.output_paused());
    assert!(terminal.write_control_output(b"\x1b[?25h\x1b[?1049l"));
    Ok(())
}

#[test]
fn adversarial_input_and_mode_changes_never_exceed_limits() -> Result<(), CreateError> {
    let limits = Limits {
        input_bytes: 64,
        line_bytes: 16,
        records: 4,
        output_bytes: 128,
        low_water: 16,
        high_water: 48,
    };
    let mut terminal = Discipline::new(Settings::default(), limits)?;
    let mut state = 0x1234_5678_u32;
    for iteration in 0..20000_u64 {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        terminal.input(&state.to_le_bytes(), iteration);
        if iteration % 11 == 0 {
            let _ = read(&mut terminal, 7, iteration);
        }
        if iteration % 37 == 0 {
            terminal.commit_output(13);
        }
        if iteration % 101 == 0 {
            let settings = if terminal.settings().canonical {
                Settings::raw()
            } else {
                Settings::default()
            };
            assert!(terminal.configure(settings).is_ok());
        }
        assert!(terminal.buffered_input() <= limits.input_bytes);
        assert!(terminal.output_len() <= limits.output_bytes);
    }
    Ok(())
}

#[test]
fn flow_profile_recovery_retries_start_after_a_full_priority_queue() -> Result<(), CreateError> {
    let mut settings = Settings::raw();
    settings.input.send_flow_control = true;
    settings.controls.start = Some(b'S');
    settings.controls.stop = Some(b'P');
    let limits = Limits {
        input_bytes: 16,
        line_bytes: 4,
        low_water: 3,
        high_water: 8,
        ..Limits::default()
    };
    let mut terminal = Discipline::new(settings, limits)?;
    terminal.input(b"abcdefgh", 1);
    terminal.commit_flow_output(1);
    let _ = read(&mut terminal, 8, 2);
    for time in 3..6 {
        terminal.input(b"abcdefgh", time);
        let _ = read(&mut terminal, 8, time);
    }
    terminal.input(b"abcdefgh", 6);
    assert!(terminal.peer_stopped());
    terminal.suspend_send_flow(true);
    terminal.resume_peer();
    assert!(terminal.peer_stopped());
    terminal.commit_flow_output(8);
    terminal.resume_peer();
    assert!(!terminal.peer_stopped());
    let mut bytes = [0; 8];
    assert_eq!(terminal.flow_output(&mut bytes), 1);
    assert_eq!(bytes[0], b'S');
    Ok(())
}

#[test]
fn full_record_queue_drops_new_line_without_publishing_its_suffix() -> Result<(), CreateError> {
    let limits = Limits {
        records: 1,
        ..Limits::default()
    };
    let mut terminal = Discipline::new(Settings::default(), limits)?;
    terminal.input(b"first\nuntrusted-prefix", 1);
    assert_eq!(read(&mut terminal, 64, 2).1, b"first\n");
    terminal.input(b"-suffix\nnext\n", 3);
    assert_eq!(read(&mut terminal, 64, 4).1, b"next\n");
    assert_eq!(read(&mut terminal, 64, 5).0, ReadDecision::Pending);
    assert_ne!(terminal.take_events().0 & EventFlags::INPUT_LOSS, 0);
    Ok(())
}

#[test]
fn malformed_utf8_erase_removes_one_byte_without_eating_valid_prefix() -> Result<(), CreateError> {
    let mut terminal = terminal(Settings::default())?;
    terminal.input(b"ok\xc3\xa9\x80\x7f\x7f\xe2\x82\x7f\n", 1);
    assert_eq!(read(&mut terminal, 32, 2).1, b"ok\xe2\n");
    Ok(())
}

#[test]
fn output_conversion_is_nonrecursive_and_retains_old_queued_encoding() -> Result<(), CreateError> {
    let mut settings = Settings::raw();
    settings.output.cr_to_lf = true;
    settings.output.lf_to_crlf = true;
    let mut terminal = terminal(settings)?;
    assert_eq!(terminal.write_output(b"a\r\n"), 3);
    assert!(terminal.configure(Settings::raw()).is_ok());
    assert_eq!(terminal.write_output(b"\r\n"), 2);
    let mut output = [0; 16];
    let count = terminal.output(&mut output);
    assert_eq!(&output[..count], b"a\n\r\n\r\n");
    Ok(())
}

#[test]
fn owner_recovery_clears_local_stops_but_keeps_remote_flow_obligation() -> Result<(), CreateError> {
    let mut settings = Settings::raw();
    settings.input.receive_flow_control = true;
    settings.input.send_flow_control = true;
    let limits = Limits {
        input_bytes: 16,
        line_bytes: 4,
        low_water: 3,
        high_water: 8,
        ..Limits::default()
    };
    let mut terminal = Discipline::new(settings, limits)?;
    terminal.input(b"\x13abcdefgh", 1);
    terminal.set_output_paused(true);
    assert!(terminal.output_paused());
    assert!(terminal.peer_stopped());
    assert_eq!(terminal.write_output(b"old"), 3);
    terminal.resume_output();
    assert!(!terminal.output_paused());
    assert!(terminal.peer_stopped());
    assert!(terminal.write_control_output(b"reset"));
    let mut output = [0; 16];
    let count = terminal.output(&mut output);
    assert_eq!(&output[..count], b"oldreset");
    Ok(())
}

#[test]
fn untouched_raw_input_uses_arrival_timing_and_does_not_commit_on_poll() {
    let mut settings = Settings::raw();
    settings.read_policy = ReadPolicy::InterByteTimeout {
        minimum: 4,
        gap_ns: 10,
    };
    let mut read = ReadRequest::new(settings, 8, 1, u64::MAX);
    assert_eq!(read.poll_raw(0, 0, 0, 100), ReadDecision::Pending);
    assert_eq!(read.poll_raw(1, 101, 1, 102), ReadDecision::Pending);
    assert_eq!(read.next_deadline(), 111);
    assert_eq!(read.poll_raw(2, 109, 2, 110), ReadDecision::Pending);
    assert_eq!(read.next_deadline(), 119);
    assert_eq!(read.poll_raw(2, 109, 2, 119), ReadDecision::Data(2));
    // A blocked transport reply leaves the same bytes and decision available.
    assert_eq!(read.poll_raw(2, 109, 2, 120), ReadDecision::Data(2));
}

#[test]
fn binary_demand_profile_excludes_input_policy_but_not_output_conversion() -> Result<(), CreateError>
{
    let mut settings = Settings::raw();
    assert!(terminal(settings)?.binary_input());
    settings.output.lf_to_crlf = true;
    assert!(terminal(settings)?.binary_input());
    settings.echo_newline = true;
    assert!(!terminal(settings)?.binary_input());
    settings.echo_newline = false;
    settings.input.receive_flow_control = true;
    assert!(!terminal(settings)?.binary_input());
    Ok(())
}
