# Kafka Simulator - Python Implementation from Scratch

A lightweight, educational implementation of Apache Kafka's core concepts built entirely in Python without any external dependencies.

## Features

✅ **Topics** - Create and manage message topics  
✅ **Partitions** - Distribute messages across multiple partitions  
✅ **Producers** - Send messages to topics with key-based partitioning  
✅ **Consumers** - Read messages from topics  
✅ **Consumer Groups** - Coordinate multiple consumers with offset management  
✅ **Offset Tracking** - Automatic and manual offset commit  
✅ **Thread-Safe** - Built-in locking for concurrent access  
✅ **Hash-based Partitioning** - Messages with the same key go to the same partition

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Kafka Broker                          │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Topic: "events"                                       │ │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐  │ │
│  │  │ Partition 0  │ │ Partition 1  │ │ Partition 2  │  │ │
│  │  │              │ │              │ │              │  │ │
│  │  │ Msg(off=0)   │ │ Msg(off=0)   │ │ Msg(off=0)   │  │ │
│  │  │ Msg(off=1)   │ │ Msg(off=1)   │ │ Msg(off=1)   │  │ │
│  │  │ Msg(off=2)   │ │ Msg(off=2)   │ │ Msg(off=2)   │  │ │
│  │  └──────────────┘ └──────────────┘ └──────────────┘  │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
         ▲                                        │
         │ Produce                                │ Consume
         │                                        ▼
  ┌──────────────┐                      ┌──────────────────┐
  │  Producer    │                      │ Consumer Group   │
  └──────────────┘                      │  (offset mgmt)   │
                                        └──────────────────┘
```

## Quick Start

### Basic Usage

```python
from kafka_simulator import KafkaBroker, Producer, Consumer

# 1. Create a broker
broker = KafkaBroker()

# 2. Create a topic
broker.create_topic("events", num_partitions=3)

# 3. Create a producer and send messages
producer = Producer(broker)
producer.send("events", {"user_id": 1, "action": "login"}, key="user-1")

# 4. Create a consumer and read messages
consumer = Consumer(broker, group_id="my-group")
consumer.subscribe(["events"])
messages = consumer.poll(max_messages=10)

for msg in messages:
    print(f"Received: {msg.value}")
```

## Core Concepts

### 1. Topics and Partitions

Topics are divided into partitions for parallelism and scalability:

```python
# Create a topic with 4 partitions
broker.create_topic("user-events", num_partitions=4)

# Messages with the same key always go to the same partition
producer.send("user-events", data, key="user-123")  # Hash-based routing
producer.send("user-events", data, partition=2)      # Explicit partition
```

### 2. Message Ordering

- **Within a partition**: Messages are strictly ordered
- **Across partitions**: No ordering guarantee
- **Same key**: Always goes to the same partition (ordering preserved)

### 3. Consumer Groups

Multiple consumers can work together to process messages:

```python
# Two consumers in the same group share the workload
consumer1 = Consumer(broker, group_id="analytics")
consumer2 = Consumer(broker, group_id="analytics")

# Each consumer in a different group gets ALL messages
consumer3 = Consumer(broker, group_id="monitoring")
```

### 4. Offset Management

Offsets track which messages have been consumed:

```python
# Auto-commit (default)
consumer = Consumer(broker, group_id="my-group", auto_commit=True)

# Manual commit
consumer = Consumer(broker, auto_commit=False)
messages = consumer.poll()
# ... process messages ...
consumer.commit()

# Seek to specific offset
consumer.seek("events", partition=0, offset=100)
```

## Advanced Examples

### Multi-threaded Producers and Consumers

```python
import threading

def producer_thread(broker, topic, producer_id, num_messages):
    producer = Producer(broker)
    for i in range(num_messages):
        producer.send(topic, {"msg": i}, key=f"key-{producer_id}")

# Create multiple producers
threads = []
for i in range(3):
    t = threading.Thread(target=producer_thread, args=(broker, "events", i, 100))
    threads.append(t)
    t.start()

for t in threads:
    t.join()
```

### Stream Processing Pipeline

```python
# Producer -> Topic A
producer.send("raw-events", raw_data)

# Processor: Consume from Topic A, process, produce to Topic B
consumer = Consumer(broker, group_id="processor")
consumer.subscribe(["raw-events"])

messages = consumer.poll()
for msg in messages:
    processed = transform(msg.value)
    producer.send("processed-events", processed)

# Final consumer reads from Topic B
final_consumer = Consumer(broker, group_id="analytics")
final_consumer.subscribe(["processed-events"])
```

### Partition Assignment by Key

```python
# All messages with the same key go to the same partition
for user_id in range(1000):
    producer.send("user-events", 
                  {"user_id": user_id, "action": "click"}, 
                  key=f"user-{user_id}")

# Messages for user-123 will always be in the same partition,
# maintaining ordering for that user
```

## API Reference

### KafkaBroker

```python
broker = KafkaBroker()
broker.create_topic(name, num_partitions=3)
broker.get_topic(name)
broker.list_topics()
```

### Producer

```python
producer = Producer(broker)
producer.send(topic_name, value, key=None, partition=None)
producer.send_batch(topic_name, [(key, value), ...])
```

### Consumer

```python
consumer = Consumer(broker, group_id="my-group", auto_commit=True)
consumer.subscribe(["topic1", "topic2"])
messages = consumer.poll(max_messages=10, timeout=1.0)
consumer.commit()
consumer.seek(topic, partition, offset)
```

### Message

```python
message.key          # Message key
message.value        # Message value (your data)
message.offset       # Offset in partition
message.partition    # Partition number
message.topic        # Topic name
message.timestamp    # Time when produced
```

## Key Differences from Real Kafka

This is an educational simulator, not production-ready. Key differences:

| Feature | Real Kafka | This Simulator |
|---------|-----------|----------------|
| Storage | Disk-based, persistent | In-memory only |
| Replication | Multi-broker replication | Single-broker |
| Clustering | Distributed system | Single process |
| Guarantees | Exactly-once, at-least-once | Best-effort |
| Performance | Handles millions of msgs/sec | Educational use only |
| Network | TCP protocol | In-process |

## Use Cases

✅ **Learning Kafka concepts** - Understand topics, partitions, offsets  
✅ **Testing** - Mock Kafka for unit tests  
✅ **Prototyping** - Quickly test streaming architectures  
✅ **Education** - Teaching distributed systems concepts  

❌ **Production use** - Use real Apache Kafka instead  
❌ **High throughput** - Not optimized for performance  
❌ **Persistence** - Data lost on restart  

## Running the Examples

```bash
# Basic demo
python kafka_simulator.py

# Advanced examples
python kafka_advanced_examples.py
```

## Implementation Details

### Thread Safety

All critical sections use locks:
- Partition append operations
- Offset management
- Topic creation
- Consumer group coordination

### Partitioning Strategy

```python
def get_partition(key):
    if key is None:
        return random_partition()
    else:
        return hash(key) % num_partitions
```

### Offset Storage

- Consumer groups maintain a mapping: `{topic: {partition: offset}}`
- Each consumer tracks its current position independently
- Commits update the group's shared offset state

## Future Enhancements

Possible additions (educational purposes):

- [ ] Message retention policies
- [ ] Compaction
- [ ] Transactions
- [ ] Exactly-once semantics
- [ ] Consumer rebalancing
- [ ] Metrics and monitoring
- [ ] Time-based message expiration

## License

Educational use - feel free to modify and extend!

## Learn More

To understand real Apache Kafka:
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka: The Definitive Guide](https://www.confluent.io/resources/kafka-the-definitive-guide/)
