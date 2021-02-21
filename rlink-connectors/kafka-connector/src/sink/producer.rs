use std::borrow::BorrowMut;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::ClientConfig;
use rlink::channel::handover::Handover;
use rlink::channel::TryRecvError;
use rlink::utils::EMPTY_SLICE;

use crate::KafkaRecord;

#[derive(Clone)]
pub struct KafkaProducerThread {
    topic: String,
    producer: FutureProducer,
    handover: Handover,

    drain_counter: Arc<AtomicU64>,
    discard_counter: Arc<AtomicU64>,
}

impl KafkaProducerThread {
    pub fn new(topic: String, client_config: ClientConfig, handover: Handover) -> Self {
        let producer: FutureProducer = client_config.create().expect("Consumer creation failed");

        KafkaProducerThread {
            topic,
            producer,
            handover,
            drain_counter: Arc::new(AtomicU64::new(0)),
            discard_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn run(&mut self) {
        let idle_delay_10 = Duration::from_millis(10);
        let idle_delay_300 = Duration::from_millis(300);
        let mut idle_counter = 0;

        let batch = 3000;

        loop {
            let mut future_queue = Vec::with_capacity(batch);
            let mut discard_counter = 0;
            for _n in 0..batch {
                match self.handover.try_poll_next() {
                    Ok(mut record) => {
                        let mut reader = KafkaRecord::new(record.borrow_mut());

                        let timestamp = reader.get_kafka_timestamp().unwrap_or_default();
                        let key = reader.get_kafka_key().unwrap_or(&EMPTY_SLICE).to_vec();
                        let payload = reader.get_kafka_payload().unwrap_or(&EMPTY_SLICE);

                        let future_record = FutureRecord::to(self.topic.as_str())
                            .payload(payload)
                            .timestamp(timestamp as i64)
                            .key(&key);

                        match self.producer.send_result(future_record) {
                            Ok(delivery_future) => future_queue.push(delivery_future),
                            Err((e, _future_record)) => {
                                error!("send error. {}", e);
                                discard_counter += 1;
                            }
                        }
                    }
                    Err(TryRecvError::Empty) => {
                        break;
                    }
                    Err(TryRecvError::Disconnected) => {
                        panic!("kafka recv channel disconnected");
                    }
                }
            }

            if future_queue.len() == 0 {
                idle_counter += 1;
                if idle_counter < 30 {
                    tokio::time::sleep(idle_delay_10).await;
                } else {
                    tokio::time::sleep(idle_delay_300).await;
                }
            } else {
                idle_counter = 0;
                self.producer.flush(Duration::from_secs(3));

                let mut drain_counter = 0;
                for future in future_queue {
                    match future.await {
                        Ok(result) => match result {
                            Ok((_, _)) => drain_counter += 1,
                            Err((err, _msg)) => {
                                error!("produce error: {:?}", err);
                                discard_counter += 1;
                            }
                        },
                        Err(e) => {
                            error!("produce `Canceled` error. {}", e);
                            discard_counter += 1;
                        }
                    }
                }

                self.drain_counter
                    .fetch_add(drain_counter as u64, Ordering::Relaxed);
            }

            if discard_counter > 0 {
                self.discard_counter
                    .fetch_add(discard_counter as u64, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use rdkafka::ClientConfig;
    use rlink::api::element::Record;
    use rlink::channel::handover::Handover;
    use rlink::utils::date_time::current_timestamp_millis;

    use crate::sink::producer::KafkaProducerThread;
    use crate::{build_kafka_record, BOOTSTRAP_SERVERS};

    fn get_record() -> Record {
        build_kafka_record(
            current_timestamp_millis() as i64,
            "abc".as_bytes(),
            "bbbbbbbbbbbbbbbbbbbbbbbbbbb".as_bytes(),
            "",
            0,
            0,
        )
        .unwrap()
    }

    #[tokio::test(threaded_scheduler)]
    pub async fn producer2_result_test() {
        let topic = "rust-demo";

        let mut client_config = ClientConfig::new();
        client_config.set(BOOTSTRAP_SERVERS, "localhost:9092");

        let handover = Handover::new("test", vec![], 32);

        let handover_c = handover.clone();
        std::thread::spawn(move || {
            let record = get_record();
            for _n in 0..1000000 {
                handover_c.produce(record.clone()).unwrap();
            }
            println!("finish");
        });

        let mut kafka_producer =
            KafkaProducerThread::new(topic.to_string(), client_config, handover);

        let kafka_producer_clone = kafka_producer.clone();
        std::thread::spawn(move || loop {
            if kafka_producer_clone.drain_counter.load(Ordering::Relaxed) == 1000000 {
                println!(
                    "end... {}",
                    rlink::utils::date_time::current_timestamp_millis()
                );
                break;
            }
        });

        println!(
            "being... {}",
            rlink::utils::date_time::current_timestamp_millis()
        );

        kafka_producer.run().await;
    }
}
