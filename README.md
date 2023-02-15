# Muffin

Muffin is a background job processing library designed to work with MongoDB as its storage backend.

*Requires MongoDB 6 with feature compatibility level set to `6`.*

- [Documentation](https://docs.rs/muffin)
- [Crates.io](https://crates.io/crates/muffin)

## Example

```rust
// Connect to a database called "muffin_testing".
let client = Client::with_uri_str("mongodb://localhost:27017").await?;
let database = client.database("muffin_testing");

// The state is passed to each "process" method on an instance of Job.
let state = JobState {
    mongo_client: client,
};

let mut muffin = Muffin::new(database, Arc::new(state)).await?;

// Create a new job and push it for processing
muffin
    .push(MyJob {
        name: "Peter".into(),
    })
    .await?;

// Register jobs that should be processed.
muffin.register::<MyJob>();

// Start processing jobs.
tokio::select! {
    _ = muffin.run() => {},
    _ = tokio::signal::ctrl_c() => {
        eprintln!("Received ctrl+c!");
    }
};

// Need to wait for all in-flight jobs to finish processing.
muffin.shutdown().await;
```

## License

- MIT license
