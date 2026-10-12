use super::ObservationSink;
use crate::observations::CSV_HEADER;

/// `KINFERENCE_OBSERVATIONS_CSV` is process-global and cargo runs tests in
/// threads, so the three tests below have to take turns: without this one
/// test's `remove_var` lands between another's `set_var` and its
/// `from_env()`, and the sink comes back disabled. That was an intermittent
/// failure in roughly one run in five.
static ENV: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Ignores poisoning — a panicking test leaves the env var set, which the
/// next test overwrites anyway, so there is no state to protect.
fn env_guard() -> std::sync::MutexGuard<'static, ()> {
    ENV.lock().unwrap_or_else(|e| e.into_inner())
}

#[test]
fn observation_sink_disabled_when_env_var_unset() {
    let _guard = env_guard();
    // SAFETY: tests in the same binary share env state; `ENV` serializes them.
    unsafe {
        std::env::remove_var("KINFERENCE_OBSERVATIONS_CSV");
    }
    let sink = ObservationSink::from_env();
    sink.append_line("anything");
}

#[test]
fn observation_sink_writes_header_on_fresh_file() {
    let _guard = env_guard();
    // Distinct path per test so parallel runs don't trample.
    let tmp = std::env::temp_dir().join(format!("kinference-csv-fresh-{}.log", std::process::id()));
    let _ = std::fs::remove_file(&tmp);
    unsafe {
        std::env::set_var("KINFERENCE_OBSERVATIONS_CSV", &tmp);
    }
    let sink = ObservationSink::from_env();
    sink.append_line("row-one");
    sink.append_line("row-two");
    unsafe {
        std::env::remove_var("KINFERENCE_OBSERVATIONS_CSV");
    }
    let contents = std::fs::read_to_string(&tmp).expect("sink file written");
    let expected = format!("{CSV_HEADER}\nrow-one\nrow-two\n");
    assert_eq!(contents, expected, "got: {contents:?}");
    let _ = std::fs::remove_file(&tmp);
}

#[test]
fn observation_sink_appends_silently_to_existing_file() {
    let _guard = env_guard();
    // Distinct path per test so parallel runs don't trample.
    let tmp = std::env::temp_dir().join(format!(
        "kinference-csv-existing-{}.log",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&tmp);
    // Pre-seed the file with content. The sink should NOT prepend
    // a header on open (would corrupt the existing schema /
    // double-header an in-progress run).
    std::fs::write(&tmp, "pre-existing-row\n").expect("seed");

    unsafe {
        std::env::set_var("KINFERENCE_OBSERVATIONS_CSV", &tmp);
    }
    let sink = ObservationSink::from_env();
    sink.append_line("new-row");
    unsafe {
        std::env::remove_var("KINFERENCE_OBSERVATIONS_CSV");
    }

    let contents = std::fs::read_to_string(&tmp).expect("sink file readable");
    assert_eq!(contents, "pre-existing-row\nnew-row\n", "got: {contents:?}");
    let _ = std::fs::remove_file(&tmp);
}
