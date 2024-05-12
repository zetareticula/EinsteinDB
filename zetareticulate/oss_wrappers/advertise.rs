// Copyright 2024 The Google Research Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::io::Write;
use std::process::{Command, Stdio};


// Define a struct to hold the status code and message


#[derive(Debug)]
struct Status {
    code: i32, // assuming i32 for error code for simplicity
    message: String,
}


impl Status {
    fn new(code: i32, message: &str) -> Self {
        Status {
            code,
            message: message.to_string(),
        }
    }
}

// Define a builder struct to build the status object with additional information

#[derive(Debug)]
struct StatusBuilder {
    status: Status,
    streamptr: Option<std::string::String>,
}

impl StatusBuilder {
    fn new(status: Status) -> Self {
        StatusBuilder {
            status,
            streamptr: None,
        }
    }

    fn log_error(&self) -> &Self {
        self
    }

    fn create_status(mut self) -> Status {
        if let Some(streamptr) = self.streamptr.take() {
            let new_msg = format!("{}; {}", self.status.message, streamptr);
            self.status.message = new_msg;
        }
        self.status
    }
}

// Implement Display trait for StatusBuilder
impl fmt::Display for StatusBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.status)
    }
}

fn aborted_error_builder() -> StatusBuilder {
    StatusBuilder::new(Status::new(1, "")) // assuming error code 1 for aborted error
}

fn already_exists_error_builder() -> StatusBuilder {
    StatusBuilder::new(Status::new(2, "")) // assuming error code 2 for already exists error
}

fn cancelled_error_builder() -> StatusBuilder {
    StatusBuilder::new(Status::new(3, "")) // assuming error code 3 for cancelled error
}

// Implement other error builders in a similar manner...

fn main() {
    let aborted_error = aborted_error_builder().log_error().create_status();
    println!("Aborted error: {}", aborted_error);

    let already_exists_error = already_exists_error_builder()
        .log_error()
        .create_status();
    println!("Already exists error: {}", already_exists_error);

    let cancelled_error = cancelled_error_builder().log_error().create_status();
    println!("Cancelled error: {}", cancelled_error);
}
