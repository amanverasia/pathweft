#[test]
fn three_process_desktop_demo() {
    let result = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../scripts/demo.py"
        ))
        .arg("--binary")
        .arg(env!("CARGO_BIN_EXE_pathweft"))
        .output()
        .expect("python3 is required for the process-level demo test");
    assert!(
        result.status.success(),
        "demo failed:\n{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn durable_outbox_survives_process_crashes() {
    let result = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../scripts/outbox_demo.py"
        ))
        .arg("--binary")
        .arg(env!("CARGO_BIN_EXE_pathweft"))
        .output()
        .expect("python3 is required for the process-level outbox test");
    assert!(
        result.status.success(),
        "outbox proof failed:\n{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn relay_mailbox_delivers_after_restart_and_deduplicates_after_lost_ack() {
    check_mailbox(&[]);
}

#[test]
fn relay_mailbox_counts_hops_and_delivers_over_discovered_routes() {
    check_mailbox(&["--discovered"]);
}

fn check_mailbox(args: &[&str]) {
    let result = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../scripts/mailbox_demo.py"
        ))
        .arg("--binary")
        .arg(env!("CARGO_BIN_EXE_pathweft"))
        .args(args)
        .output()
        .expect("python3 is required for the relay mailbox test");
    assert!(
        result.status.success(),
        "relay mailbox proof failed:\n{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}
