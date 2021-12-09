use std::fmt::Debug;
use crate::traits::{Gettable, Message, Sendable};
use crate::logger::Logger;

use futures::StreamExt;
use log::{error, info, warn};
use ratsio::StanClient;
use serde_json::Value;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

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
    pub async fn subscribe<G, E, P, R>(self, sender: Sender<Box<dyn Message<Params=P, Return=R>>>)
    where
        E: Debug,
        G: Gettable<Error=E, MessageParams=P, MessageReturn=R>,
    {
        match self
            .stan_client
            .subscribe(G::topic(), G::queue_group(), G::durable_name())
            .await
        {
            Err(error) => {
                error!(
                    "Fail to subscribe by topic: {} with error: {:?}",
                    G::topic(),
                    error
                );
                self.status.store(false, Ordering::SeqCst);
            }
            Ok((stan_id, mut stream)) => {
                info!("Successfully subscribe by topic: {}", G::topic());

                loop {
                    select! {
                        () = Self::is_alive(Arc::clone(&self.status)) => break,
                        option = stream.next() => {
                            if let Some(raw_message) = option {
                                match G::parse(&raw_message.payload) {
                                    Ok(message) => {
                                        Logger::got_message(&G::topic(), &message);
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
                    Ok(_) => warn!("Connection by topic: {} closed!", G::topic()),
                    Err(error) => error!(
                        "Error while unsubscribe by topic: {} from nats: {:?}",
                        G::topic(),
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
                () = Self::is_alive(Arc::clone(&self.status)) => break,
                option = rx.recv() => {
                    if let Some(message) = option {
                        match self
                            .stan_client
                            .publish(&message.get_topic(), &message.get_bytes())
                            .await {
                            Ok(_) => Logger::sent_message(&message.get_topic(), message.get_message()),
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

    async fn is_alive(status: Arc<AtomicBool>) {
        while status.load(Ordering::Acquire) {
            sleep(Duration::from_millis(10000)).await;
        }
    }
}
