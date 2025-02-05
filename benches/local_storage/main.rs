use criterion::{criterion_group, criterion_main, Criterion};
use nix::sys::stat::Mode;
use sealfs::server::storage_engine::{
    default_engine::{self, DefaultEngine},
    StorageEngine,
};

fn create_file(engine: &DefaultEngine, n: isize) {
    let mode = Mode::S_IRUSR
        | Mode::S_IWUSR
        | Mode::S_IRGRP
        | Mode::S_IWGRP
        | Mode::S_IROTH
        | Mode::S_IWOTH;
    (0..n).for_each(|i| {
        engine.create_file(i.to_string(), mode).unwrap();
    })
}

fn delete_file(engine: &DefaultEngine, n: isize) {
    (0..n).for_each(|i| {
        engine.delete_file(i.to_string()).unwrap();
    })
}

fn write_file(engine: &DefaultEngine, n: isize) {
    (0..n).for_each(|i| {
        engine.write_file(i.to_string(), b"hello world", 0).unwrap();
    })
}

fn criterion_benchmark(c: &mut Criterion) {
    let engine = default_engine::DefaultEngine::new("/tmp/bench/db", "/tmp/bench/root");

    c.bench_function("default engine file 100", |b| {
        b.iter(|| {
            create_file(&engine, 100);
            write_file(&engine, 100);
            delete_file(&engine, 100);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
