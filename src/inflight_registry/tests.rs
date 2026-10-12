use super::*;

#[test]
fn inc_then_get_returns_one() {
    let r = InflightRegistry::new();
    r.inc("a:1");
    assert_eq!(r.get("a:1"), 1);
}

#[test]
fn three_inc_two_dec_returns_one() {
    let r = InflightRegistry::new();
    r.inc("a:1");
    r.inc("a:1");
    r.inc("a:1");
    r.dec("a:1");
    r.dec("a:1");
    assert_eq!(r.get("a:1"), 1);
}

#[test]
fn dec_at_zero_saturates() {
    let r = InflightRegistry::new();
    r.dec("a:1"); // no entry yet
    assert_eq!(r.get("a:1"), 0);
    r.inc("a:1");
    r.dec("a:1");
    r.dec("a:1"); // already at 0
    r.dec("a:1"); // still 0
    assert_eq!(r.get("a:1"), 0);
}

#[test]
fn distinct_endpoints_dont_interfere() {
    let r = InflightRegistry::new();
    r.inc("a:1");
    r.inc("b:2");
    r.inc("b:2");
    assert_eq!(r.get("a:1"), 1);
    assert_eq!(r.get("b:2"), 2);
    assert_eq!(r.get("c:3"), 0);
}

#[test]
fn get_on_missing_returns_zero() {
    let r = InflightRegistry::new();
    assert_eq!(r.get("never-seen"), 0);
}

#[tokio::test]
async fn concurrent_inc_dec_balances() {
    let r = std::sync::Arc::new(InflightRegistry::new());
    let mut handles = vec![];
    // 16 tasks each do 1000 inc + 1000 dec on the same endpoint.
    // Net delta == 0, final count must be 0.
    for _ in 0..16 {
        let r = r.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..1000 {
                r.inc("hot:1");
            }
            for _ in 0..1000 {
                r.dec("hot:1");
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(r.get("hot:1"), 0);
}
