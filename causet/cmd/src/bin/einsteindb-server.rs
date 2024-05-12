// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

#![feature(proc_macro_hygiene)]

use std::path::Path;
use std::process;

use clap::{crate_authors, App, Arg};
use cmd::setup::{ensure_no_unrecognized_config, validate_and_persist_config};
use edb::config::EINSTEINDBConfig;

fn main() {
    let version_info = edb::edb_version_info();

    let matches = App::new("EinsteinDB")
        .about("A distributed transactional key-value database powered by Rust and VioletaBft")
        .author(crate_authors!())
        .version(version_info.as_ref())
        .long_version(version_info.as_ref())
        .arg(
            Arg::with_name("config")
                .short("C")
                .long("config")
                .value_name("FILE")
                .help("Set the configuration file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("config-check")
                .required(false)
                .long("config-check")
                .takes_value(false)
                .help("Check config file validity and exit"),
        )
        .arg(
            Arg::with_name("log-level")
                .short("L")
                .long("log-level")
                .alias("log")
                .takes_value(true)
                .value_name("LEVEL")
                .possible_values(&[
                    "trace", "debug", "info", "warn", "warning", "error", "critical",
                ])
                .help("Set the log level"),
        )
        .arg(
            Arg::with_name("log-file")
                .short("f")
                .long("log-file")
                .takes_value(true)
                .value_name("FILE")
                .help("Sets log file")
                .long_help("Set the log file path. If not set, logs will output to stderr"),
        )
        .arg(
            Arg::with_name("addr")
                .short("A")
                .long("addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Set the listening address"),
        )
        .arg(
            Arg::with_name("advertise-addr")
                .long("advertise-addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Set the advertise listening address for client communication"),
        )
        .arg(
            Arg::with_name("status-addr")
                .long("status-addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Set the HTTP listening address for the status report service"),
        )
        .arg(
            Arg::with_name("advertise-status-addr")
                .long("advertise-status-addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Set the advertise listening address for the client communication of status report service"),
        )
        .arg(
            Arg::with_name("data-dir")
                .long("data-dir")
                .short("s")
                .alias("store")
                .takes_value(true)
                .value_name("PATH")
                .help("Set the directory used to store data"),
        )
        .arg(
            Arg::with_name("capacity")
                .long("capacity")
                .takes_value(true)
                .value_name("CAPACITY")
                .help("Set the store capacity")
                .long_help("Set the store capacity to use. If not set, use entire partition"),
        )
        .arg(
            Arg::with_name("fidel-lightlikepoints")
                .long("fidel-lightlikepoints")
                .aliases(&["fidel", "fidel-lightlikepoint"])
                .takes_value(true)
                .value_name("FIDel_URL")
                .multiple(true)
                .use_delimiter(true)
                .require_delimiter(true)
                .value_delimiter(",")
                .help("Sets FIDel lightlikepoints")
                .long_help("Set the FIDel lightlikepoints to use. Use `,` to separate multiple FIDels"),
        )
        .arg(
            Arg::with_name("labels")
                .long("labels")
                .alias("label")
                .takes_value(true)
                .value_name("KEY=VALUE")
                .multiple(true)
                .use_delimiter(true)
                .require_delimiter(true)
                .value_delimiter(",")
                .help("Sets server labels")
                .long_help(
                    "Set the server labels. Uses `,` to separate kv pairs, like \
                     `zone=cn,disk=ssd`",
                ),
        )
        .arg(
            Arg::with_name("print-sample-config")
                .long("print-sample-config")
                .help("Print a sample config to stdout"),
        )
        .arg(
            Arg::with_name("metrics-addr")
                .long("metrics-addr")
                .value_name("IP:PORT")
                .help("Sets Prometheus Pushgateway address")
                .long_help(
                    "Sets push address to the Prometheus Pushgateway, \
                     leaves it empty will disable Prometheus push",
                ),
        )
        .get_matches();

    if matches.is_present("print-sample-config") {
        let config = EINSTEINDBConfig::default();
        println!("{}", toml::to_string_pretty(&config).unwrap());
        process::exit(0);
    }

    let mut unrecognized_tuplespaceInstanton = Vec::new();
    let is_config_check = matches.is_present("config-check");

    let mut config = matches
        .value_of_os("config")
        .map_or_else(EINSTEINDBConfig::default, |path| {
            EINSTEINDBConfig::from_file(
                Path::new(path),
                if is_config_check {
                    Some(&mut unrecognized_tuplespaceInstanton)
                } else {
                    None
                },
            )
        });

    cmd::setup::overwrite_config_with_cmd_args(&mut config, &matches);

    if is_config_check {
        validate_and_persist_config(&mut config, false);
        ensure_no_unrecognized_config(&unrecognized_tuplespaceInstanton);
        println!("config check successful");
        process::exit(0)
    }

    cmd::server::run_edb(config);
}
