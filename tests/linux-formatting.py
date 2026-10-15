#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 roolrz
# SPDX-License-Identifier: Apache-2.0

"""Check the shared formatting vectors against Linux's N_TTY on actual PTYs."""

import os
from pathlib import Path
import select
import sys
import termios
import time


def decode(value):
    return b'' if value == '-' else bytes.fromhex(value)


def collect(fd, expected):
    data = bytearray()
    deadline = time.monotonic() + 2
    while True:
        timeout = max(0, deadline - time.monotonic()) if len(data) < expected else 0.05
        if not select.select([fd], [], [], timeout)[0]:
            return bytes(data)
        chunk = os.read(fd, 4096)
        if not chunk:
            raise EOFError('terminal closed while collecting formatting output')
        data.extend(chunk)
        if len(data) >= 4096:
            raise AssertionError('unexpected unbounded terminal output')


def configure(slave, delta):
    attrs = termios.tcgetattr(slave)
    # IUTF8 is a Linux input flag, absent from some Python termios builds.
    attrs[0] = termios.ICRNL | termios.IXON | 0x4000
    attrs[1] = termios.OPOST | termios.ONLCR | termios.ONLRET
    attrs[3] = (termios.ICANON | termios.IEXTEN | termios.ECHO | termios.ECHOCTL |
                termios.ECHOE | termios.ECHOK | termios.ECHOKE)
    for index, value in (
        (termios.VEOF, 4), (termios.VERASE, 127), (termios.VKILL, 21),
        (termios.VWERASE, 23), (termios.VLNEXT, 22), (termios.VREPRINT, 18),
        (termios.VEOL, 0), (termios.VEOL2, 0), (termios.VSTART, 17),
        (termios.VSTOP, 19), (termios.VMIN, 1), (termios.VTIME, 0),
    ):
        attrs[6][index] = bytes([value])
    flags = {
        'canonical': (3, termios.ICANON), 'echo': (3, termios.ECHO),
        'echo_controls': (3, termios.ECHOCTL), 'echo_newline': (3, termios.ECHONL),
        'visual_erase': (3, termios.ECHOE), 'echo_kill_line': (3, termios.ECHOK),
        'visual_kill': (3, termios.ECHOKE), 'echo_erased': (3, termios.ECHOPRT),
        'strip_high_bit': (0, termios.ISTRIP), 'lowercase': (0, termios.IUCLC),
        'ignore_cr': (0, termios.IGNCR), 'lf_to_cr': (0, termios.INLCR),
        'cr_to_lf': (1, termios.OCRNL), 'expand_tabs': (1, termios.TAB3),
        'uppercase': (1, termios.OLCUC),
        'suppress_cr_at_column_zero': (1, termios.ONOCR),
    }
    if delta != '-':
        for item in delta.split(','):
            index, flag = flags[item[1:]]
            if item[0] == '+':
                attrs[index] |= flag
            else:
                attrs[index] &= ~flag
    termios.tcsetattr(slave, termios.TCSANOW, attrs)


def main():
    if sys.platform != 'linux':
        print('Linux PTY formatting reference: skipped (requires Linux N_TTY)')
        return
    count = 0
    for row in Path(__file__).with_name('formatting.tsv').read_text().splitlines():
        if row.startswith('#'):
            continue
        name, direction, delta, source, data, output = row.split('\t')
        master, slave = os.openpty()
        try:
            configure(slave, delta)
            os.write(master if direction == 'input' else slave, decode(source))
            if direction == 'input':
                actual = collect(slave, len(decode(data)))
                assert actual == decode(data), (name, 'read', actual, decode(data))
            actual = collect(master, len(decode(output)))
            assert actual == decode(output), (name, 'output', actual, decode(output))
        finally:
            os.close(slave)
            os.close(master)
        count += 1
    print(f'verified {count} formatting vectors against Linux PTYs')


if __name__ == '__main__':
    main()
