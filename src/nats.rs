use crate::traits::{Gettable, Message, Sendable};

use rust_lib::healthchecker::HealthChecker;
use rust_lib::logger::nats::NatsLoggerWrapper;

use futures::StreamExt;
use log::{error, info, warn};
use ratsio::StanClient;
use serde_json::Value;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct Nats {
    stan_client: Arc<StanClient>,
    status: Arc<AtomicBool>,
}

impl Nats {
    #[must_use]
    pub fn new(stan_client: Arc<StanClient>, status: Arc<AtomicBool>) -> Self {
        Self {
            stan_client,
            status,
        }
    }

    /// # Panics
    pub async fn subscribe<T, U, E>(self, sender: Sender<Box<dyn Message<U, E>>>)
    where
        T: Gettable<U, E>,
    {
        match self
            .stan_client
            .subscribe(T::topic(), T::queue_group(), T::durable_name())
            .await
        {
            Err(error) => {
                error!(
                    "Fail to subscribe by topic: {} with error: {:?}",
                    T::topic(),
                    error
                );
                self.status.store(false, Ordering::SeqCst);
            }
            Ok((stan_id, mut stream)) => {
                info!("Successfully subscribe by topi: {}", T::topic());

                loop {
                    select! {
                        () = HealthChecker::is_alive(Arc::clone(&self.status)) => break,
                        option = stream.next() => {
                            if let Some(raw_message) = option {
                                match T::parse(&raw_message.payload) {
                                    Ok(message) => {
                                        NatsLoggerWrapper::got_message(&T::topic(), &message);
                                        if sender.send(message).await.is_err() {
                                            self.status.store(false, Ordering::SeqCst);
                                        }
                                    }
                                    Err(error) => error!("Error: {:?}\n while parsing this json: {:?}", error, serde_json::from_slice::<Value>(&raw_message.payload)),
                                }
                            }
                        }
                    }
                }

                match self.stan_client.un_subscribe(&stan_id).await {
                    Ok(_) => warn!("Connection by topic: {} closed!", T::topic()),
                    Err(error) => error!(
                        "Error while unsubscribe by topic: {} from nats: {:?}",
                        T::topic(),
                        error
                    ),
                }
            }
        }
    }

    /// # Panics
    pub async fn response(self, mut rx: Receiver<Box<dyn Sendable>>) {
        loop {
            select! {
                () = HealthChecker::is_alive(Arc::clone(&self.status)) => break,
                option = rx.recv() => {
                    if let Some(message) = option {
                        match self
                            .stan_client
                            .publish(&message.get_topic(), &message.get_bytes())
                            .await {
                            Ok(_) => NatsLoggerWrapper::sent_message(&message.get_topic(), message.get_message()),
                            Err(error) => {
                                error!("Send to nats error: {:?}", error);
                                self.status.store(false, Ordering::SeqCst);
                            }
                        }
                    }
                }
            }
        }
        warn!("Responder terminated!");
    }
}
