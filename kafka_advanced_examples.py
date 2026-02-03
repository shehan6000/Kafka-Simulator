"""
Advanced Kafka Simulator Examples
Demonstrates threading, multiple producers/consumers, and real-world patterns
"""

import threading
import time
import random
from kafka_simulator import KafkaBroker, Producer, Consumer


def producer_thread(broker, topic_name, producer_id, num_messages):
    """Simulate a producer sending messages"""
    producer = Producer(broker)
    
    for i in range(num_messages):
        message = {
            "producer_id": producer_id,
            "message_num": i,
            "data": f"Message {i} from producer {producer_id}",
            "timestamp": time.time()
        }
        
        key = f"key-{random.randint(1, 5)}"
        producer.send(topic_name, message, key=key)
        time.sleep(random.uniform(0.1, 0.3))
    
    print(f"Producer {producer_id} finished sending {num_messages} messages")


def consumer_thread(broker, topic_name, group_id, consumer_id, duration):
    """Simulate a consumer reading messages"""
    consumer = Consumer(broker, group_id=group_id)
    consumer.subscribe([topic_name])
    
    start_time = time.time()
    total_messages = 0
    
    while time.time() - start_time < duration:
        messages = consumer.poll(max_messages=5)
        total_messages += len(messages)
        
        if messages:
            print(f"Consumer {consumer_id} (group={group_id}) received {len(messages)} messages")
        
        time.sleep(0.5)
    
    print(f"Consumer {consumer_id} finished. Total messages consumed: {total_messages}")


def example_multi_producer_consumer():
    """Example with multiple producers and consumers"""
    print("\n" + "=" * 70)
    print("EXAMPLE: Multiple Producers and Consumers")
    print("=" * 70)
    
    broker = KafkaBroker()
    broker.create_topic("events", num_partitions=4)
    
    # Create multiple producer threads
    producer_threads = []
    for i in range(3):
        t = threading.Thread(target=producer_thread, args=(broker, "events", i, 10))
        producer_threads.append(t)
        t.start()
    
    # Give producers a head start
    time.sleep(0.5)
    
    # Create multiple consumer threads in different groups
    consumer_threads = []
    
    # Group 1: Two consumers sharing the load
    for i in range(2):
        t = threading.Thread(target=consumer_thread, 
                           args=(broker, "events", "group-1", f"1-{i}", 5))
        consumer_threads.append(t)
        t.start()
    
    # Group 2: One consumer reading all messages
    t = threading.Thread(target=consumer_thread, 
                        args=(broker, "events", "group-2", "2-0", 5))
    consumer_threads.append(t)
    t.start()
    
    # Wait for all threads to complete
    for t in producer_threads:
        t.join()
    
    for t in consumer_threads:
        t.join()
    
    print("\nAll threads completed!")


def example_partition_assignment():
    """Example showing partition assignment with keys"""
    print("\n" + "=" * 70)
    print("EXAMPLE: Partition Assignment with Keys")
    print("=" * 70)
    
    broker = KafkaBroker()
    broker.create_topic("partitioned-topic", num_partitions=3)
    
    producer = Producer(broker)
    
    # Send messages with specific keys to see partitioning
    keys = ["user-A", "user-B", "user-C", "user-A", "user-B"]
    
    print("\nSending messages with keys:")
    for i, key in enumerate(keys):
        msg = producer.send("partitioned-topic", f"Message {i}", key=key)
        print(f"  Key='{key}' -> Partition {msg.partition}")
    
    # Show that same keys go to same partitions
    print("\nVerifying consistency - same key goes to same partition:")
    for _ in range(3):
        msg = producer.send("partitioned-topic", "Another message", key="user-A")
        print(f"  Key='user-A' -> Partition {msg.partition}")


def example_offset_management():
    """Example showing manual offset management"""
    print("\n" + "=" * 70)
    print("EXAMPLE: Manual Offset Management")
    print("=" * 70)
    
    broker = KafkaBroker()
    broker.create_topic("offset-topic", num_partitions=1)
    
    # Produce some messages
    producer = Producer(broker)
    for i in range(10):
        producer.send("offset-topic", f"Message {i}", key=f"key-{i}")
    
    print("\n1. Reading with auto-commit:")
    consumer1 = Consumer(broker, group_id="offset-group", auto_commit=True)
    consumer1.subscribe(["offset-topic"])
    
    messages = consumer1.poll(max_messages=5)
    print(f"   Read {len(messages)} messages (offsets {messages[0].offset}-{messages[-1].offset})")
    
    print("\n2. Creating new consumer in same group:")
    consumer2 = Consumer(broker, group_id="offset-group")
    consumer2.subscribe(["offset-topic"])
    
    messages = consumer2.poll(max_messages=5)
    print(f"   Read {len(messages)} messages (offsets {messages[0].offset}-{messages[-1].offset})")
    print("   (Started from where previous consumer left off)")
    
    print("\n3. Seeking to specific offset:")
    consumer3 = Consumer(broker, group_id="new-group")
    consumer3.subscribe(["offset-topic"])
    consumer3.seek("offset-topic", 0, 3)  # Seek to offset 3
    
    messages = consumer3.poll(max_messages=5)
    print(f"   Read {len(messages)} messages starting from offset {messages[0].offset}")


def example_stream_processing():
    """Example simulating stream processing"""
    print("\n" + "=" * 70)
    print("EXAMPLE: Stream Processing Pipeline")
    print("=" * 70)
    
    broker = KafkaBroker()
    broker.create_topic("raw-events", num_partitions=2)
    broker.create_topic("processed-events", num_partitions=2)
    
    # Producer: Generate raw events
    print("\nGenerating raw events...")
    producer = Producer(broker)
    
    for i in range(20):
        event = {
            "event_id": i,
            "value": random.randint(1, 100),
            "type": random.choice(["click", "view", "purchase"])
        }
        producer.send("raw-events", event, key=f"event-{i}")
    
    # Processor: Read, transform, and write
    print("\nProcessing events...")
    consumer = Consumer(broker, group_id="processor-group")
    consumer.subscribe(["raw-events"])
    
    processor_producer = Producer(broker)
    
    messages = consumer.poll(max_messages=20)
    processed_count = 0
    
    for msg in messages:
        # Transform the event
        processed_event = {
            "original": msg.value,
            "processed_value": msg.value["value"] * 2,
            "processed_at": time.time()
        }
        
        processor_producer.send("processed-events", processed_event, key=msg.key)
        processed_count += 1
    
    print(f"Processed {processed_count} events")
    
    # Final consumer: Read processed results
    print("\nReading processed events...")
    final_consumer = Consumer(broker, group_id="analytics-group")
    final_consumer.subscribe(["processed-events"])
    
    results = final_consumer.poll(max_messages=20)
    print(f"Retrieved {len(results)} processed events")
    
    # Show sample
    if results:
        print("\nSample processed event:")
        print(f"  {results[0].value}")


if __name__ == "__main__":
    # Run all examples
    example_partition_assignment()
    time.sleep(1)
    
    example_offset_management()
    time.sleep(1)
    
    example_stream_processing()
    time.sleep(1)
    
    example_multi_producer_consumer()
    
    print("\n" + "=" * 70)
    print("All examples completed!")
    print("=" * 70)
