use super::*;

/// `chain` joins the error and every cause with ": ".
#[test]
fn chain_joins_the_causes() {
    let source = serde_json::from_str::<serde_json::Value>("nope").expect_err("not JSON");
    let err = Error::RenderDecode {
        what: "decoding /render response as JSON",
        source,
    };
    let chain = err.chain();
    assert!(
        chain.starts_with("decoding /render response as JSON: "),
        "{chain}"
    );
    assert!(chain.len() > err.to_string().len() + 2, "{chain}");
    assert_eq!(Error::RenderShape("empty".into()).chain(), "empty");
}
