//! The recorded double's contract, which the loops' tests rely on.

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use kube::api::ObjectMeta;

use super::ApiServer;
use super::recorded::{Recorded, Write, key};

fn lease(name: &str) -> Lease {
    Lease {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some("infra".into()),
            ..Default::default()
        },
        spec: None,
    }
}

/// The HTTP status an API error carries.
fn code<T>(r: Result<T, kube::Error>) -> Option<u16> {
    match r {
        Err(kube::Error::Api(status)) => Some(status.code),
        _ => None,
    }
}

#[tokio::test]
async fn create_then_get_round_trips_and_a_second_create_conflicts() {
    let api = Recorded::default();
    assert_eq!(code(api.get::<Lease>("infra", "l").await), Some(404));

    let created = api.create("infra", &lease("l")).await.unwrap();
    assert_eq!(created.metadata.resource_version.as_deref(), Some("1"));
    let got: Lease = api.get("infra", "l").await.unwrap();
    assert_eq!(got.metadata.name.as_deref(), Some("l"));

    assert_eq!(code(api.create("infra", &lease("l")).await), Some(409));
    assert_eq!(
        api.writes(),
        vec![Write::Create(key::<Lease>("infra", "l"))]
    );
}

#[tokio::test]
async fn replace_with_a_stale_resource_version_conflicts() {
    let api = Recorded::default().with([lease("l")]);
    let stale: Lease = api.get("infra", "l").await.unwrap();

    let mut fresh = stale.clone();
    fresh.spec = Some(LeaseSpec {
        holder_identity: Some("me".into()),
        ..Default::default()
    });
    let replaced = api.replace("infra", "l", &fresh).await.unwrap();
    assert_eq!(replaced.metadata.resource_version.as_deref(), Some("2"));

    assert_eq!(code(api.replace("infra", "l", &stale).await), Some(409));
    assert_eq!(
        code(api.replace("infra", "missing", &fresh).await),
        Some(404)
    );

    // No `resourceVersion` is an unconditional replace.
    let mut blind = stale.clone();
    blind.metadata.resource_version = None;
    api.replace("infra", "l", &blind).await.unwrap();
    let now: Lease = api.object("infra", "l").unwrap();
    assert_eq!(now.spec, None);
    assert_eq!(now.metadata.resource_version.as_deref(), Some("3"));
    assert_eq!(
        api.writes(),
        vec![
            Write::Replace(key::<Lease>("infra", "l")),
            Write::Replace(key::<Lease>("infra", "l")),
        ]
    );
}

#[tokio::test]
async fn an_absent_kind_lists_as_404() {
    let api = Recorded::default().with([lease("l")]);
    let listed: Vec<Lease> = api.list(Some("infra")).await.unwrap();
    assert_eq!(listed.len(), 1);
    let elsewhere: Vec<Lease> = api.list(Some("other")).await.unwrap();
    assert!(elsewhere.is_empty());

    let api = api.without::<Lease>();
    assert_eq!(code(api.list::<Lease>(None).await), Some(404));
}

#[tokio::test]
async fn a_watch_lists_the_recording_and_stays_open() {
    use futures::StreamExt as _;
    use kube::runtime::watcher::{self, Config, Event};
    use std::time::Duration;

    let mut tagged = lease("tagged");
    tagged.metadata.labels = Some([("team".to_string(), "a".to_string())].into());
    let api = Recorded::default().with([lease("plain"), tagged]);

    let mut all = api.watch::<Lease>(Some("infra"), Config::default());
    assert!(matches!(all.next().await, Some(Ok(Event::Init))));
    let mut names = Vec::new();
    for _ in 0..2 {
        match all.next().await {
            Some(Ok(Event::InitApply(l))) => names.push(l.metadata.name.unwrap()),
            other => panic!("expected an InitApply, got {other:?}"),
        }
    }
    names.sort();
    assert_eq!(names, ["plain", "tagged"]);
    assert!(matches!(all.next().await, Some(Ok(Event::InitDone))));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), all.next())
            .await
            .is_err(),
        "the watch stays open after the list"
    );

    let selected: Vec<_> = api
        .watch::<Lease>(None, Config::default().labels("team=a"))
        .take(3)
        .collect()
        .await;
    assert!(
        matches!(&selected[1], Ok(Event::InitApply(l)) if l.metadata.name.as_deref() == Some("tagged"))
    );
    assert!(matches!(selected[2], Ok(Event::InitDone)));

    let mut absent = api
        .without::<Lease>()
        .watch::<Lease>(None, Config::default());
    assert!(matches!(
        absent.next().await,
        Some(Err(watcher::Error::InitialListFailed(kube::Error::Api(s)))) if s.code == 404
    ));
}
