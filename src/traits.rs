use serde_json::Value;
use std::fmt::Debug;
use async_trait::async_trait;

pub trait Gettable {
    type Error: Debug;
    type MessageParams;
    type MessageReturn;

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
    fn parse(payload: &[u8]) -> Result<Box<dyn Message<Params=Self::MessageParams, Return=Self::MessageReturn>>, Self::Error>;
}

pub trait Sendable: Debug + Sync + Send {
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
pub trait Message: Debug + Sync + Send {
    type Params;
    type Return;

    /// # Errors
    async fn process(&self, params: &Self::Params) -> Self::Return;

    fn into_json(self) -> Value;
}
