// Known-bad input for check_ignore_reasons.sh. Two switched-off tests: one with no
// reason at all, one whose reason names a forbidden category.
//
// Deliberately does not spell the attribute in prose — the guard's comment filter skips
// `///` and `//!` but not `//`, so a plain comment mentioning it is flagged too.

#[ignore]
#[test]
fn switched_off_with_no_reason() {}

#[ignore = "flaky, fails sometimes on CI"]
#[test]
fn switched_off_because_it_fails() {}
