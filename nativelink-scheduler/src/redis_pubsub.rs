use redis::Commands;
use redis::{ControlFlow, PubSubCommands};
use serde::{Deserialize, Serialize};
use std::error::Error;
use uuid::Uuid;

trait StructMessage {
    fn handle(&self);
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Message<T> {
    pub id: String,
    pub channel: String,
    pub payload: T,
}

pub fn generate_id() -> String {
    Uuid::new_v4().simple().to_string()
}

impl<T> Message<T>
where
    T: StructMessage,
{
    pub fn new(payload: T) -> Message<T> {
        Message {
            id: generate_id(),
            channel: String::from("order"),
            payload,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Order {
    description: String,
    quantity: u16,
    total_price: f32,
}

impl Order {
    pub fn new(description: String, quantity: u16, total_price: f32) -> Order {
        Order {
            description,
            quantity,
            total_price,
        }
    }
}

pub fn handle<T: serde::de::DeserializeOwned + std::fmt::Debug>(message: Message<T>) {
    println!("{:?}", message);
}

pub fn publish_message<T: serde::ser::Serialize>(
    message: Message<T>,
) -> Result<(), Box<dyn Error>> {
    let client = redis::Client::open("redis://localhost/")?;
    let mut con = client.get_connection()?;

    let json = serde_json::to_string(&message)?;

    con.publish(message.channel, json)?;

    Ok(())
}

pub fn subscribe<T: serde::de::DeserializeOwned + std::fmt::Debug>(
    channel: String,
) -> Result<(), Box<dyn Error>> {
    // let _ = tokio::spawn(async move {
    //     let client = redis::Client::open("redis://localhost").unwrap();
    //     let mut con = client.get_connection().unwrap();
    //     let _: () = con
    //         .subscribe(&[channel], |msg| {
    //             let received: String = msg.get_payload().unwrap();
    //             let message_obj = serde_json::from_str::<Message<T>>(&received).unwrap();
    //             handle(message_obj);
    //
    //             return ControlFlow::Continue;
    //         })
    //         .unwrap();
    // });

    Ok(())
}
