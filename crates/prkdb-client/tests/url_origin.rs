//! Dependency regression for RUSTSEC-2026-0592; no network access is required.
use url::{Origin, Url};

#[test]
fn nested_blob_origin_is_opaque_without_recursive_descent() {
    let https = concat!("https:", "//example.com/path");
    // This fails on url 2.5.8 before exercising the stack-exhausting input below.
    let nested = Url::parse(&format!("blob:blob:{https}")).unwrap();
    assert!(matches!(nested.origin(), Origin::Opaque(_)));

    let deeply_nested = Url::parse(&format!("{}{https}", "blob:".repeat(10_000))).unwrap();
    assert!(matches!(deeply_nested.origin(), Origin::Opaque(_)));
}

#[test]
fn https_and_single_blob_https_retain_their_origin() {
    let https = Url::parse(concat!("https:", "//example.com:8443/path")).unwrap();
    let blob = Url::parse(&format!("blob:{https}")).unwrap();
    assert_eq!(https.origin(), blob.origin());
    assert_eq!(
        https.origin().ascii_serialization(),
        concat!("https:", "//example.com:8443")
    );
}
