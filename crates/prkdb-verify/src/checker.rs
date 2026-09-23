//! Durable-mode check: after a restart, every key's value equals the model's.

use crate::model::Model;
use crate::sut::Sut;

#[derive(Debug)]
pub struct Mismatch {
    pub key: Vec<u8>,
    pub expected: Option<Vec<u8>>,
    pub actual: Option<Vec<u8>>,
}

/// Returns (number of keys compared, first mismatch).
pub async fn check_durable(
    model: &Model,
    sut: &mut dyn Sut,
    key_space: u8,
) -> anyhow::Result<(usize, Option<Mismatch>)> {
    let mut compared = 0;
    for b in 0..key_space {
        let key = vec![b'k', b];
        let expected = model.get(&key).cloned();
        let actual = sut.get(&key).await?;
        compared += 1;
        if expected != actual {
            return Ok((
                compared,
                Some(Mismatch {
                    key,
                    expected,
                    actual,
                }),
            ));
        }
    }
    Ok((compared, None))
}
