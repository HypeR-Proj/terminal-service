// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! One formal Native terminal. Session policy and process creation live elsewhere.

mod admission;
mod reactor;

use hyper_os::startup::Startup;

fn application_main(mut startup: Startup<'_>) -> std::process::ExitCode {
    match reactor::run(&mut startup) {
        // Session retirement revokes the raw binding before stopping the task
        // group. The manager diagnoses unexpected terminal loss itself.
        Ok(()) | Err(hyper_os::Error::Status(hyper_os::Status::PEER_CLOSED)) => {
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("terminal-service: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn main() -> std::process::ExitCode {
    match hyper_rt::process::startup() {
        Ok(startup) => application_main(startup),
        Err(_) => std::process::ExitCode::FAILURE,
    }
}
