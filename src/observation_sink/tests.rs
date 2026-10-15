use super::ObservationSink;
use crate::observations::CSV_HEADER;

#[test]
fn observation_sink_disabled_without_a_path() {
    let sink = ObservationSink::open(None);
    sink.append_line("anything");
}

#[test]
fn observation_sink_writes_header_on_fresh_file() {
    // Distinct path per test so parallel runs don't trample.
    let tmp = std::env::temp_dir().join(format!("kinference-csv-fresh-{}.log", std::process::id()));
    let _ = std::fs::remove_file(&tmp);
    let sink = ObservationSink::open(Some(tmp.clone()));
    sink.append_line("row-one");
    sink.append_line("row-two");
    let contents = std::fs::read_to_string(&tmp).expect("sink file written");
    let expected = format!("{CSV_HEADER}\nrow-one\nrow-two\n");
    assert_eq!(contents, expected, "got: {contents:?}");
    let _ = std::fs::remove_file(&tmp);
}

#[test]
fn observation_sink_appends_silently_to_existing_file() {
    // Distinct path per test so parallel runs don't trample.
    let tmp = std::env::temp_dir().join(format!(
        "kinference-csv-existing-{}.log",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&tmp);
    // Pre-seed the file. The sink must not prepend a header.
    std::fs::write(&tmp, "pre-existing-row\n").expect("seed");

    let sink = ObservationSink::open(Some(tmp.clone()));
    sink.append_line("new-row");

    let contents = std::fs::read_to_string(&tmp).expect("sink file readable");
    assert_eq!(contents, "pre-existing-row\nnew-row\n", "got: {contents:?}");
    let _ = std::fs::remove_file(&tmp);
}
