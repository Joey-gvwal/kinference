use super::*;

fn gemma() -> Vec<String> {
    vec!["gemma".to_string()]
}

#[test]
fn a_claim_is_recorded() {
    let idx = ModelIndex::new();
    idx.associate(PoolId::from_raw(0), "a", &gemma());
    assert_eq!(idx.pools_for("gemma"), vec![PoolId::from_raw(0)]);
}

/// A second pool's claim is recorded alongside the first.
#[test]
fn a_second_pool_joins_rather_than_being_ignored() {
    let idx = ModelIndex::new();
    idx.associate(PoolId::from_raw(0), "a", &gemma());
    idx.associate(PoolId::from_raw(1), "b", &gemma());
    assert_eq!(
        idx.pools_for("gemma"),
        vec![PoolId::from_raw(0), PoolId::from_raw(1)]
    );
}

/// Both arrival orders produce the same set, iterated in the same order.
#[test]
fn claim_order_does_not_affect_the_result() {
    let forward = ModelIndex::new();
    forward.associate(PoolId::from_raw(0), "a", &gemma());
    forward.associate(PoolId::from_raw(2), "c", &gemma());
    let reverse = ModelIndex::new();
    reverse.associate(PoolId::from_raw(2), "c", &gemma());
    reverse.associate(PoolId::from_raw(0), "a", &gemma());
    assert_eq!(forward.pools_for("gemma"), reverse.pools_for("gemma"));
}

#[test]
fn re_claiming_is_idempotent() {
    let idx = ModelIndex::new();
    for _ in 0..3 {
        idx.associate(PoolId::from_raw(0), "a", &gemma());
    }
    assert_eq!(idx.pools_for("gemma"), vec![PoolId::from_raw(0)]);
}

#[test]
fn distinct_models_route_to_distinct_pools() {
    let idx = ModelIndex::new();
    idx.associate(PoolId::from_raw(0), "a", &gemma());
    idx.associate(PoolId::from_raw(1), "b", &["llama".to_string()]);
    assert_eq!(idx.pools_for("gemma"), vec![PoolId::from_raw(0)]);
    assert_eq!(idx.pools_for("llama"), vec![PoolId::from_raw(1)]);
}

/// A departed pool's claim is released.
#[test]
fn releasing_a_pool_frees_the_model_for_its_replacement() {
    let idx = ModelIndex::new();
    idx.associate(PoolId::from_raw(0), "a", &gemma());
    idx.release(PoolId::from_raw(0), "x");
    assert!(
        idx.models().is_empty(),
        "a model nobody serves should not linger as an empty entry"
    );
    idx.associate(PoolId::from_raw(7), "a-v2", &gemma());
    assert_eq!(idx.pools_for("gemma"), vec![PoolId::from_raw(7)]);
}

#[test]
fn releasing_one_of_two_pools_leaves_the_other_serving() {
    let idx = ModelIndex::new();
    idx.associate(PoolId::from_raw(0), "a", &gemma());
    idx.associate(PoolId::from_raw(1), "b", &gemma());
    idx.release(PoolId::from_raw(0), "x");
    assert_eq!(idx.pools_for("gemma"), vec![PoolId::from_raw(1)]);
}

#[test]
fn releasing_an_unknown_pool_is_a_noop() {
    let idx = ModelIndex::new();
    idx.associate(PoolId::from_raw(0), "a", &gemma());
    idx.release(PoolId::from_raw(9), "x");
    assert_eq!(idx.pools_for("gemma"), vec![PoolId::from_raw(0)]);
}

#[test]
fn empty_model_list_is_noop() {
    let idx = ModelIndex::new();
    idx.associate(PoolId::from_raw(0), "a", &[]);
    assert!(idx.models().is_empty());
}

/// Each new (model, pool) pair is published once; a repeat claim is silent.
#[tokio::test]
async fn a_new_claim_is_published_once() {
    let idx = ModelIndex::new();
    let mut rx = idx.subscribe();
    idx.associate(PoolId::from_raw(0), "a", &gemma());
    idx.associate(PoolId::from_raw(0), "a", &gemma());
    idx.associate(PoolId::from_raw(1), "b", &gemma());
    assert_eq!(
        rx.try_recv().unwrap(),
        ModelClaim {
            pool: PoolId::from_raw(0),
            model: "gemma".to_string()
        }
    );
    assert_eq!(
        rx.try_recv().unwrap(),
        ModelClaim {
            pool: PoolId::from_raw(1),
            model: "gemma".to_string()
        }
    );
    assert!(
        rx.try_recv().is_err(),
        "the repeat claim must not be published"
    );
}

/// A list with one new model among known ones publishes only the new one.
#[tokio::test]
async fn only_the_new_models_of_a_list_are_published() {
    let idx = ModelIndex::new();
    idx.associate(PoolId::from_raw(0), "a", &gemma());
    let mut rx = idx.subscribe();
    idx.associate(
        PoolId::from_raw(0),
        "a",
        &["gemma".to_string(), "llama".to_string()],
    );
    assert_eq!(rx.try_recv().unwrap().model, "llama");
    assert!(rx.try_recv().is_err());
}
