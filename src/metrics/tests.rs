use super::*;

/// A substring check would let `... 200` pass for `... 20`.
fn assert_line(out: &str, expect: &str) {
    let name = expect.split('{').next().unwrap();
    let got = out
        .lines()
        .find(|l| l.starts_with(name) && l.starts_with(&expect[..expect.rfind(' ').unwrap()]));
    assert_eq!(
        got,
        Some(expect),
        "\nexpected line: {expect}\nrendered:\n{out}"
    );
}

/// Build identity is exported, and agrees with what the snapshot reports.
///
/// The value is always 1; the information is in the labels. Present on a
/// fresh registry, since it is set at construction.
#[test]
fn build_info_reports_the_version_at_construction() {
    let out = render(&registry());
    assert_line(
        &out,
        &format!(
            r#"kinference_build_info{{version="{}"}} 1"#,
            crate::version::VERSION
        ),
    );
    // The same string `--version` shows.
    assert!(
        !crate::version::VERSION.is_empty(),
        "an empty version label would be worse than none"
    );
}
