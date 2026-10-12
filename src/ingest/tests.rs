use super::*;

// ─── BlockSizeGuard debounce + sanity bound ─────────────────────

#[test]
fn matching_block_size_is_healthy() {
    let mut g = BlockSizeGuard::new(16);
    assert_eq!(g.classify(16), BlockSizeVerdict::Healthy);
}

#[test]
fn implausible_value_is_a_glitch_never_evicts() {
    // The reported incident: a misframed event with block_size=1568.
    let mut g = BlockSizeGuard::new(16);
    // Even repeated, an out-of-bounds value never trips eviction and
    // never advances the debounce counter.
    for _ in 0..10 {
        assert_eq!(g.classify(1568), BlockSizeVerdict::Glitch);
    }
    assert_eq!(g.consecutive, 0);
    assert_eq!(g.classify(0), BlockSizeVerdict::Glitch);
}

#[test]
fn single_plausible_mismatch_does_not_evict() {
    let mut g = BlockSizeGuard::new(16);
    assert_eq!(g.classify(32), BlockSizeVerdict::Pending { consecutive: 1 });
}

#[test]
fn sustained_same_mismatch_evicts_at_threshold() {
    let mut g = BlockSizeGuard::new(16);
    assert_eq!(g.classify(32), BlockSizeVerdict::Pending { consecutive: 1 });
    assert_eq!(g.classify(32), BlockSizeVerdict::Pending { consecutive: 2 });
    // Third consecutive identical mismatch (threshold = 3) → evict.
    assert_eq!(g.classify(32), BlockSizeVerdict::Evict);
}

#[test]
fn a_matching_event_resets_the_debounce() {
    let mut g = BlockSizeGuard::new(16);
    g.classify(32);
    g.classify(32);
    assert_eq!(g.classify(16), BlockSizeVerdict::Healthy);
    // Counter reset — the next mismatch starts over at 1, not 3.
    assert_eq!(g.classify(32), BlockSizeVerdict::Pending { consecutive: 1 });
}

#[test]
fn varied_garbage_never_accumulates_to_eviction() {
    // Distinct plausible mismatching values in a row shouldn't evict:
    // a real misconfig reports ONE consistent wrong value.
    let mut g = BlockSizeGuard::new(16);
    assert_eq!(g.classify(32), BlockSizeVerdict::Pending { consecutive: 1 });
    assert_eq!(g.classify(64), BlockSizeVerdict::Pending { consecutive: 1 });
    assert_eq!(
        g.classify(128),
        BlockSizeVerdict::Pending { consecutive: 1 }
    );
    // The counter tracks the current value only; nothing reached 3.
}

#[test]
fn parse_seq_payload_extracts_seq_and_payload() {
    let msg = vec![
        b"topic".to_vec(),
        42u64.to_be_bytes().to_vec(),
        b"hello".to_vec(),
    ];
    let (seq, payload) = parse_seq_payload(&msg).expect("3-frame msg parses");
    assert_eq!(seq, 42);
    assert_eq!(payload, b"hello");
}

#[test]
fn parse_seq_payload_rejects_wrong_frame_count() {
    assert!(parse_seq_payload(&[]).is_none());
    assert!(parse_seq_payload(&[b"a".to_vec()]).is_none());
    assert!(parse_seq_payload(&[b"a".to_vec(), b"b".to_vec()]).is_none());
    let four = vec![b"a".to_vec(); 4];
    assert!(parse_seq_payload(&four).is_none());
}

#[test]
fn parse_seq_payload_rejects_malformed_seq() {
    let msg = vec![
        b"topic".to_vec(),
        vec![1, 2, 3], // wrong length, not 8 bytes
        b"payload".to_vec(),
    ];
    assert!(parse_seq_payload(&msg).is_none());
}

/// Cursor state machine: a fresh listener starts at `Initial`;
/// after applying seq=10, it's `Live(10)`; seq=11 is
/// `Contiguous`; seq=9 is `Stale`; seq=20 reports a `Gap`.
/// Direct test of dynamo's `CursorState` integration — the core
/// invariant of the new design.
#[test]
fn cursor_state_transitions_through_initial_live_gap_stale() {
    let mut cursor = CursorState::Initial;
    // First observation: Initial { got: 10 } when starting fresh
    // at a non-zero seq.
    assert!(matches!(
        cursor.observe(10),
        CursorObservation::Initial { got: 10 }
    ));
    // After applying, advance to Live(10).
    cursor = cursor.advance_to(10);
    assert_eq!(cursor, CursorState::Live(10));

    // Contiguous next.
    assert!(matches!(
        cursor.observe(11),
        CursorObservation::Contiguous { got: 11 }
    ));
    cursor = cursor.advance_to(11);

    // Earlier seq → Stale.
    assert!(matches!(
        cursor.observe(9),
        CursorObservation::Stale {
            got: 9,
            last_applied: Some(11)
        }
    ));
    // Cursor not advanced after Stale.
    assert_eq!(cursor, CursorState::Live(11));

    // Skip ahead → Gap.
    assert!(matches!(
        cursor.observe(20),
        CursorObservation::Gap {
            expected: 12,
            got: 20
        }
    ));
}

/// After a barrier (cache Cleared event) the cursor is
/// invalidated; the next live frame observation reports
/// `FreshAfterBarrier` rather than `Gap`, so we don't trigger an
/// inline replay against a barrier-invalidated history.
#[test]
fn barrier_invalidates_cursor_and_fresh_observation_is_not_gap() {
    let cursor = CursorState::Live(50);
    let invalidated = cursor.invalidate_by_barrier();
    assert!(matches!(
        invalidated.observe(100),
        CursorObservation::FreshAfterBarrier {
            got: 100,
            last_before_barrier: Some(50)
        }
    ));
    // Frames at or before the barrier are stale (don't reapply).
    assert!(matches!(
        invalidated.observe(50),
        CursorObservation::Stale {
            got: 50,
            last_applied: Some(50)
        }
    ));
}
