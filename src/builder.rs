use crate::nats::Nats;
use crate::traits::{Gettable, Message, Sendable};

use futures::Future;
use ratsio::{RatsioError, StanClient, StanOptions};
use tokio::sync::mpsc::{Receiver, Sender};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[allow(clippy::module_name_repetitions)]
pub struct NatsBuilder {
    stan_client: Arc<StanClient>,
    status: Arc<AtomicBool>,
}

impl NatsBuilder {
    /// # Errors
    pub async fn new(
        client_id: String,
        cluster_id: String,
        address: String,
        status: Arc<AtomicBool>,
    ) -> Result<Self, RatsioError> {
        let mut opts = StanOptions::with_options(address, cluster_id, client_id);
        opts.nats_options.subscribe_on_reconnect = false;

        let stan_client = StanClient::from_options(opts).await?;

        Ok(Self {
            stan_client,
            status,
        })
    }

    /// # Errors
    pub async fn add_default_reconnect_handler(&self) -> Result<(), RatsioError> {
        let status_for_reconnect = Arc::clone(&self.status);
        self.stan_client
            .nats_client
            .add_reconnect_handler(Box::new(move |_nats_client| {
                status_for_reconnect.store(false, Ordering::SeqCst);
            }))
            .await
    }

    pub fn build_listener<T, U, E>(
        &self,
        sender: Sender<Box<dyn Message<U, E>>>,
    ) -> impl Future<Output = ()>
    where
        T: Gettable<U, E>,
    {
        let listener = Nats::new(Arc::clone(&self.stan_client), Arc::clone(&self.status));

        listener.subscribe::<T, U, E>(sender)
    }

    pub fn build_responder(&self, rx: Receiver<Box<dyn Sendable>>) -> impl Future<Output = ()> {
        let responder = Nats::new(Arc::clone(&self.stan_client), Arc::clone(&self.status));

        responder.response(rx)
    }
}
