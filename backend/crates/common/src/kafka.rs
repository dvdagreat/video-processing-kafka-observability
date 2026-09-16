use std::env;

use anyhow::{Context, Result};
use rskafka::client::{
    partition::{PartitionClient, UnknownTopicHandling},
    Client, ClientBuilder,
};

pub fn broker_list() -> Vec<String> {
    env::var("KAFKA_BROKERS")
        .unwrap_or_else(|_| "localhost:29092".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .collect()
}

pub async fn client() -> Result<Client> {
    ClientBuilder::new(broker_list())
        .build()
        .await
        .context("failed to build kafka client")
}

pub async fn ensure_topic(client: &Client, topic: &str, num_partitions: i32) -> Result<()> {
    if topic_exists(client, topic).await? {
        return Ok(());
    }
    let controller = client
        .controller_client()
        .context("failed to get kafka controller client")?;
    match controller
        .create_topic(topic, num_partitions, 1, 5_000)
        .await
    {
        Ok(()) => Ok(()),
        Err(err) => {
            if topic_exists(client, topic).await? {
                Ok(())
            } else {
                Err(err).context(format!("failed to create topic {topic}"))
            }
        }
    }
}

async fn topic_exists(client: &Client, topic: &str) -> Result<bool> {
    let topics = client.list_topics().await.context("failed to list topics")?;
    Ok(topics.iter().any(|t| t.name == topic))
}

pub async fn partition_client(
    client: &Client,
    topic: &str,
    partition: i32,
) -> Result<PartitionClient> {
    client
        .partition_client(topic, partition, UnknownTopicHandling::Retry)
        .await
        .context("failed to get kafka partition client")
}
