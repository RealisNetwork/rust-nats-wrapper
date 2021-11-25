use serde_json::Value;
use std::fmt::Debug;
use async_trait::async_trait;

pub trait Gettable<T, E> {
    type Error: Debug;

    fn topic() -> String;

    #[must_use]
    fn queue_group() -> Option<String> {
        None
    }

    #[must_use]
    fn durable_name() -> Option<String> {
        None
    }

    /// # Errors
    fn parse(payload: &[u8]) -> Result<Box<dyn Message<T, E>>, Self::Error>;
}

pub trait Sendable {
    fn get_topic(&self) -> String;

    fn get_message(&self) -> Value;

    fn get_bytes(&self) -> Vec<u8>;
}

impl Sendable for (Value, String) {
    fn get_topic(&self) -> String {
        self.1.clone()
    }

    fn get_message(&self) -> Value {
        self.0.clone()
    }

    fn get_bytes(&self) -> Vec<u8> {
        self.0.to_string().as_bytes().to_vec()
    }
}

#[async_trait]
pub trait Message<T, E>: Debug + Sync + Send {
    /// # Errors
    async fn process(&self, params: T) -> Result<Vec<Box<dyn Sendable>>, E>;
}
