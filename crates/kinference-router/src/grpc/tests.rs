use super::*;

/// A provider that declares no path sends the empty string, meaning "leave
/// the path alone".
#[test]
fn a_provider_destination_carries_the_path_and_empty_means_unchanged() {
    let pick = |path: Option<&str>| crate::service::ProviderPick {
        provider: kinference_policy::engine::ObjectKey::new("infra", "together"),
        backend_ref: kinference_policy::engine::ObjectKey::new("infra", "together-backend"),
        path: path.map(str::to_string),
        upstream_model: "acme/llama-70b".into(),
        canonical_model: "bar".into(),
    };

    let proto::pick_response::Destination::Provider(t) =
        provider_destination(&pick(Some("/serving-endpoints/chat/completions")))
    else {
        panic!("a provider destination");
    };
    assert_eq!(t.path, "/serving-endpoints/chat/completions");
    // The backend is still named by reference and fully qualified; the path
    // adds to that rather than replacing any part of it.
    let backend = t.backend_ref.expect("backendRef");
    assert_eq!(backend.namespace, "infra");
    assert_eq!(backend.name, "together-backend");

    let proto::pick_response::Destination::Provider(t) = provider_destination(&pick(None)) else {
        panic!("a provider destination");
    };
    assert_eq!(t.path, "", "no declared path sends no path");
}
