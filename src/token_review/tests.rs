use super::*;

fn cache() -> ReviewCache {
    ReviewCache::new(Duration::from_secs(60), Duration::from_secs(10), 100)
}

fn key(token: &str, audiences: &[&str]) -> (String, Vec<String>) {
    (
        hash_token(token),
        audiences.iter().map(|s| s.to_string()).collect(),
    )
}

fn ok(username: &str) -> Review {
    Review {
        authenticated: true,
        username: username.into(),
    }
}

#[test]
fn a_hit_returns_the_cached_answer() {
    let mut c = cache();
    let now = Instant::now();
    c.insert(key("tok", &["aud"]), ok("system:serviceaccount:ns:sa"), now);
    assert_eq!(
        c.get(&key("tok", &["aud"]), now),
        Some(ok("system:serviceaccount:ns:sa"))
    );
}

/// The same token is a different question for a different audience — a
/// token minted for audience A must not be answered from A's cached
/// positive when asked about B.
#[test]
fn audiences_are_part_of_the_key() {
    let mut c = cache();
    let now = Instant::now();
    c.insert(key("tok", &["aud-a"]), ok("u"), now);
    assert!(c.get(&key("tok", &["aud-b"]), now).is_none());
}

/// Argument order is not semantics: two callers naming the same audiences
/// differently should share an entry rather than each paying a round-trip.
/// (The handler sorts before building the key; this pins the assumption the
/// cache relies on.)
#[test]
fn sorted_audiences_collapse_to_one_entry() {
    let mut c = cache();
    let now = Instant::now();
    let mut a = vec!["b".to_string(), "a".to_string()];
    a.sort();
    let mut b = vec!["a".to_string(), "b".to_string()];
    b.sort();
    c.insert((hash_token("tok"), a), ok("u"), now);
    assert!(c.get(&(hash_token("tok"), b), now).is_some());
}

/// A denial expires sooner than a positive, so a token that has just been
/// granted starts working promptly instead of being locked out for the full
/// positive TTL.
#[test]
fn a_denial_expires_sooner_than_a_positive() {
    let mut c = cache();
    let now = Instant::now();
    c.insert(key("good", &["aud"]), ok("u"), now);
    c.insert(key("bad", &["aud"]), Review::denied(), now);

    let later = now + Duration::from_secs(30);
    assert!(
        c.get(&key("bad", &["aud"]), later).is_none(),
        "a 10s negative must be gone by 30s"
    );
    assert!(
        c.get(&key("good", &["aud"]), later).is_some(),
        "a 60s positive must survive to 30s"
    );
}

#[test]
fn an_expired_entry_is_a_miss() {
    let mut c = cache();
    let now = Instant::now();
    c.insert(key("tok", &["aud"]), ok("u"), now);
    assert!(
        c.get(&key("tok", &["aud"]), now + Duration::from_secs(61))
            .is_none()
    );
}

/// Bound ServiceAccount tokens ROTATE, so an unbounded cache grows one entry
/// per rotation per caller and never shrinks. This is the difference from
/// the reference implementation, and it only shows up in a long-lived
/// process.
#[test]
fn the_cache_stays_bounded_under_token_rotation() {
    let mut c = ReviewCache::new(Duration::from_secs(3600), Duration::from_secs(10), 8);
    let now = Instant::now();
    for i in 0..500 {
        c.insert(key(&format!("rotated-token-{i}"), &["aud"]), ok("u"), now);
    }
    assert!(c.len() <= 8, "cache grew to {} entries", c.len());
}

/// Expired entries go without waiting for the cap, so the steady-state
/// footprint tracks live tokens rather than the high-water mark.
#[test]
fn expired_entries_are_swept_on_insert() {
    let mut c = ReviewCache::new(Duration::from_secs(60), Duration::from_secs(10), 1000);
    let now = Instant::now();
    for i in 0..50 {
        c.insert(key(&format!("old-{i}"), &["aud"]), ok("u"), now);
    }
    assert_eq!(c.len(), 50);
    c.insert(
        key("fresh", &["aud"]),
        ok("u"),
        now + Duration::from_secs(61),
    );
    assert_eq!(c.len(), 1, "the 50 expired entries should be gone");
}

/// The process should not hold credentials it does not need, and a
/// non-cryptographic hash would additionally let a caller construct a
/// collision against someone else's cached positive.
#[test]
fn the_key_does_not_contain_the_token() {
    let token = "eyJhbGciOiJSUzI1NiIsImtpZCI6InNlY3JldCJ9.payload.sig";
    let hashed = hash_token(token);
    assert!(!hashed.contains("eyJhbGciOi"));
    assert!(!hashed.contains("payload"));
    assert_eq!(hashed.len(), 64, "hex sha256");
    assert_ne!(
        hashed,
        hash_token("eyJhbGciOiJSUzI1NiIsImtpZCI6InNlY3JldCJ9.payload.si")
    );
}

/// `max_entries = 0` would make every insert evict the entry it just added.
#[test]
fn a_zero_cap_is_clamped_rather_than_breaking_inserts() {
    let mut c = ReviewCache::new(Duration::from_secs(60), Duration::from_secs(10), 0);
    let now = Instant::now();
    c.insert(key("tok", &["aud"]), ok("u"), now);
    assert!(c.get(&key("tok", &["aud"]), now).is_some());
}

/// The `[token_review]` section still binds to this subsystem's config type.
///
/// `crate::config::Config` does not set `deny_unknown_fields`, so a section
/// that stops binding — a renamed field, a type moved without its serde name,
/// a typo — deserializes to defaults SILENTLY and the router comes up with the
/// service quietly off. There is no compiler error and no startup error to
/// catch it, so the only guard is asserting the bound values here, from inside
/// the module that can see the private fields.
#[test]
fn token_review_section_binds_to_this_subsystems_config() {
    let with = r#"
listen = "127.0.0.1:50051"
mock_mode = true

[token_review]
enabled = true
positive_ttl_secs = 123
max_cache_entries = 7
"#;
    let cfg: crate::config::Config = toml::from_str(with).expect("parses");
    assert!(cfg.token_review.enabled);
    assert_eq!(cfg.token_review.positive_ttl_secs, 123);
    assert_eq!(cfg.token_review.max_cache_entries, 7);
    // Unset keys still fall back to this module's defaults, not to zero.
    assert_eq!(cfg.token_review.negative_ttl_secs, 10);
    assert_eq!(cfg.token_review.default_deadline_ms, 2000);

    // And absence means off with every default intact, rather than the section
    // being ignored in both directions and the above passing for the wrong reason.
    let without = r#"
listen = "127.0.0.1:50051"
mock_mode = true
"#;
    let cfg: crate::config::Config = toml::from_str(without).expect("parses");
    assert!(
        !cfg.token_review.enabled,
        "absent section must default to off"
    );
    assert_eq!(cfg.token_review.positive_ttl_secs, 60);
}
