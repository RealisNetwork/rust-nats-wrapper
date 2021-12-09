use std::fmt::Debug;
use crate::traits::{Gettable, Message, Sendable};
use crate::logger::Logger;

use futures::StreamExt;
use log::{error, info, warn};
use ratsio::StanClient;
use serde_json::Value;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use rust_lib::healthchecker::HealthChecker;

use std::sync::Arc;

pub struct Nats {
    stan_client: Arc<StanClient>,
    health_checker: HealthChecker,
}

impl Nats {
    #[must_use]
    pub fn new(stan_client: Arc<StanClient>, health_checker: HealthChecker) -> Self {
        Self {
            stan_client,
            health_checker,
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
                self.health_checker.make_sick();
            }
            Ok((stan_id, mut stream)) => {
                info!("Successfully subscribe by topic: {}", G::topic());

                loop {
                    let health_checker = self.health_checker.clone();
                    select! {
                        () = health_checker.is_alive() => break,
                        option = stream.next() => {
                            if let Some(raw_message) = option {
                                match G::parse(&raw_message.payload) {
                                    Ok(message) => {
                                        Logger::got_message(&G::topic(), &message);
                                        if sender.send(message).await.is_err() {
                                            self.health_checker.make_sick();
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
            let health_checker = self.health_checker.clone();
            select! {
                () = health_checker.is_alive() => break,
                option = rx.recv() => {
                    if let Some(message) = option {
                        match self
                            .stan_client
                            .publish(&message.get_topic(), &message.get_bytes())
                            .await {
                            Ok(_) => Logger::sent_message(&message.get_topic(), message.get_message()),
                            Err(error) => {
                                error!("Send to nats error: {:?}", error);
                                self.health_checker.make_sick();
                            }
                        }
                    }
                }
            }
        }
        warn!("Responder terminated!");
    }
}
