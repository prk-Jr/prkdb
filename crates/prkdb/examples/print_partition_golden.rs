//! Prints the KEY-03 golden vectors. Run once when the vectors are created:
//! `cargo run -p prkdb --example print_partition_golden`.
use prkdb::partitioning::{DefaultPartitioner, Partitioner};
fn main() {
    let p = DefaultPartitioner::<String>::new();
    for i in 0..10 {
        let k = format!("user-{i}");
        println!("(\"{k}\", {}),", p.partition(&k, 1024));
    }
    println!(
        "const GOLDEN_U64_42: u32 = {};",
        DefaultPartitioner::<u64>::new().partition(&42u64, 1024)
    );
}
