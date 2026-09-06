// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkDB` benchmark command-surface tests.

mod common;

use std::process::Command;

use common::direct::crowdb_cli_bin;

#[test]
fn allocate_and_mix_expose_placement_and_concurrency_controls() {
    for workload in ["allocate", "mix"] {
        let output = Command::new(crowdb_cli_bin())
            .args(["bench", "chunkdb", workload, "--help"])
            .output()
            .expect("run crowdb-cli ChunkDB benchmark help");
        assert!(output.status.success(), "{workload} help must succeed");
        let stdout = String::from_utf8(output.stdout).expect("help is UTF-8");
        assert!(stdout.contains("possible values: mirror, ec"));
        assert!(stdout.contains("--copy-count"));
        assert!(stdout.contains("--data-num"));
        assert!(stdout.contains("--code-num"));
        assert!(stdout.contains("--concurrency"));
        assert!(stdout.contains("--seed"));
    }
}

#[test]
fn combined_deploy_exposes_chunkdb_controls() {
    let output = Command::new(crowdb_cli_bin())
        .args(["cluster", "local-deploy", "--help"])
        .output()
        .expect("run combined deploy help");
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("help is UTF-8");
    assert!(stdout.contains("combined"));
    assert!(stdout.contains("--chunkdb-instances"));
    assert!(stdout.contains("--allow-unsafe-ec"));
}

#[test]
fn chunkio_write_exposes_large_write_controls() {
    let output = Command::new(crowdb_cli_bin())
        .args(["bench", "chunkio", "write", "--help"])
        .output()
        .expect("run crowdb-cli chunk IO benchmark help");
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("help is UTF-8");
    for option in [
        "--objects",
        "--object-size",
        "--concurrency",
        "--block-size",
        "--chunk-size",
        "--data-num",
        "--code-num",
        "--seed",
    ] {
        assert!(stdout.contains(option), "missing {option}");
    }
}
