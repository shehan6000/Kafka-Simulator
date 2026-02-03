"""
Kafka Simulator - A simplified implementation of Apache Kafka from scratch
Implements: Topics, Partitions, Producers, Consumers, Consumer Groups, and Offsets
"""

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import uuid


@dataclass
class Message:
    """Represents a message in Kafka"""
    key: Optional[str]
    value: Any
    timestamp: float
    offset: int
    partition: int
    topic: str


@dataclass
class Partition:
    """Represents a partition within a topic"""
    partition_id: int
    messages: deque = field(default_factory=deque)
    offset_counter: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)
    
    def append(self, key: Optional[str], value: Any, topic: str) -> Message:
        """Append a message to this partition"""
        with self.lock:
            message = Message(
                key=key,
                value=value,
                timestamp=time.time(),
                offset=self.offset_counter,
                partition=self.partition_id,
                topic=topic
            )
            self.messages.append(message)
            self.offset_counter += 1
            return message
    
    def read(self, offset: int, max_messages: int = 1) -> List[Message]:
        """Read messages starting from offset"""
        with self.lock:
            messages = []
            for msg in self.messages:
                if msg.offset >= offset and len(messages) < max_messages:
                    messages.append(msg)
            return messages
    
    def get_latest_offset(self) -> int:
        """Get the latest offset in this partition"""
        with self.lock:
            return self.offset_counter


@dataclass
class Topic:
    """Represents a Kafka topic with multiple partitions"""
    name: str
    num_partitions: int
    partitions: List[Partition] = field(default_factory=list)
    
    def __post_init__(self):
        """Initialize partitions"""
        self.partitions = [Partition(partition_id=i) for i in range(self.num_partitions)]
    
    def get_partition(self, key: Optional[str] = None, partition: Optional[int] = None) -> Partition:
        """Get partition based on key or explicit partition number"""
        if partition is not None:
            return self.partitions[partition % self.num_partitions]
        elif key is not None:
            # Hash-based partitioning
            return self.partitions[hash(key) % self.num_partitions]
        else:
            # Round-robin or random
            import random
            return random.choice(self.partitions)


class KafkaBroker:
    """Main Kafka broker that manages topics"""
    
    def __init__(self):
        self.topics: Dict[str, Topic] = {}
        self.consumer_groups: Dict[str, 'ConsumerGroup'] = {}
        self.lock = threading.Lock()
    
    def create_topic(self, name: str, num_partitions: int = 3) -> Topic:
        """Create a new topic"""
        with self.lock:
            if name in self.topics:
                print(f"Topic '{name}' already exists")
                return self.topics[name]
            
            topic = Topic(name=name, num_partitions=num_partitions)
            self.topics[name] = topic
            print(f"Created topic '{name}' with {num_partitions} partitions")
            return topic
    
    def get_topic(self, name: str) -> Optional[Topic]:
        """Get an existing topic"""
        return self.topics.get(name)
    
    def list_topics(self) -> List[str]:
        """List all topics"""
        return list(self.topics.keys())
    
    def get_or_create_consumer_group(self, group_id: str) -> 'ConsumerGroup':
        """Get or create a consumer group"""
        with self.lock:
            if group_id not in self.consumer_groups:
                self.consumer_groups[group_id] = ConsumerGroup(group_id, self)
            return self.consumer_groups[group_id]


class Producer:
    """Kafka producer that sends messages to topics"""
    
    def __init__(self, broker: KafkaBroker):
        self.broker = broker
    
    def send(self, topic_name: str, value: Any, key: Optional[str] = None, 
             partition: Optional[int] = None) -> Optional[Message]:
        """Send a message to a topic"""
        topic = self.broker.get_topic(topic_name)
        if not topic:
            print(f"Topic '{topic_name}' does not exist")
            return None
        
        target_partition = topic.get_partition(key=key, partition=partition)
        message = target_partition.append(key, value, topic_name)
        
        print(f"Produced: topic={topic_name}, partition={message.partition}, "
              f"offset={message.offset}, key={key}, value={value}")
        return message
    
    def send_batch(self, topic_name: str, messages: List[tuple]) -> List[Message]:
        """Send multiple messages"""
        results = []
        for msg in messages:
            if len(msg) == 2:
                key, value = msg
                results.append(self.send(topic_name, value, key))
            elif len(msg) == 3:
                key, value, partition = msg
                results.append(self.send(topic_name, value, key, partition))
        return results


class ConsumerGroup:
    """Manages consumer group coordination and offset tracking"""
    
    def __init__(self, group_id: str, broker: KafkaBroker):
        self.group_id = group_id
        self.broker = broker
        self.offsets: Dict[str, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
        self.lock = threading.Lock()
    
    def commit_offset(self, topic: str, partition: int, offset: int):
        """Commit offset for a topic partition"""
        with self.lock:
            self.offsets[topic][partition] = offset
    
    def get_offset(self, topic: str, partition: int) -> int:
        """Get committed offset for a topic partition"""
        with self.lock:
            return self.offsets[topic][partition]


class Consumer:
    """Kafka consumer that reads messages from topics"""
    
    def __init__(self, broker: KafkaBroker, group_id: Optional[str] = None, 
                 auto_commit: bool = True):
        self.broker = broker
        self.group_id = group_id or f"consumer-{uuid.uuid4()}"
        self.auto_commit = auto_commit
        self.consumer_group = broker.get_or_create_consumer_group(self.group_id)
        self.subscriptions: Dict[str, List[int]] = {}
        self.current_offsets: Dict[str, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
        
        print(f"Created consumer with group_id: {self.group_id}")
    
    def subscribe(self, topics: List[str], partitions: Optional[List[int]] = None):
        """Subscribe to topics"""
        for topic_name in topics:
            topic = self.broker.get_topic(topic_name)
            if not topic:
                print(f"Topic '{topic_name}' does not exist")
                continue
            
            if partitions:
                self.subscriptions[topic_name] = partitions
            else:
                # Subscribe to all partitions
                self.subscriptions[topic_name] = list(range(topic.num_partitions))
            
            # Initialize offsets from consumer group
            for partition_id in self.subscriptions[topic_name]:
                committed_offset = self.consumer_group.get_offset(topic_name, partition_id)
                self.current_offsets[topic_name][partition_id] = committed_offset
        
        print(f"Subscribed to topics: {topics}")
    
    def poll(self, max_messages: int = 10, timeout: float = 1.0) -> List[Message]:
        """Poll for messages"""
        messages = []
        
        for topic_name, partition_ids in self.subscriptions.items():
            topic = self.broker.get_topic(topic_name)
            if not topic:
                continue
            
            for partition_id in partition_ids:
                partition = topic.partitions[partition_id]
                current_offset = self.current_offsets[topic_name][partition_id]
                
                # Read messages from this partition
                new_messages = partition.read(current_offset, max_messages - len(messages))
                messages.extend(new_messages)
                
                # Update current offset
                if new_messages:
                    self.current_offsets[topic_name][partition_id] = new_messages[-1].offset + 1
                
                if len(messages) >= max_messages:
                    break
            
            if len(messages) >= max_messages:
                break
        
        # Auto-commit if enabled
        if self.auto_commit and messages:
            self.commit()
        
        return messages
    
    def commit(self):
        """Commit current offsets"""
        for topic_name, partition_offsets in self.current_offsets.items():
            for partition_id, offset in partition_offsets.items():
                self.consumer_group.commit_offset(topic_name, partition_id, offset)
        print(f"Committed offsets for group {self.group_id}")
    
    def seek(self, topic: str, partition: int, offset: int):
        """Seek to a specific offset"""
        if topic in self.subscriptions and partition in self.subscriptions[topic]:
            self.current_offsets[topic][partition] = offset
            print(f"Seeking to offset {offset} for {topic}:{partition}")


# Example usage and demonstration
def demo():
    """Demonstrate the Kafka simulator"""
    print("=" * 70)
    print("KAFKA SIMULATOR DEMO")
    print("=" * 70)
    
    # Create broker
    broker = KafkaBroker()
    
    # Create topics
    print("\n--- Creating Topics ---")
    broker.create_topic("user-events", num_partitions=3)
    broker.create_topic("orders", num_partitions=2)
    
    # Create producer
    print("\n--- Producing Messages ---")
    producer = Producer(broker)
    
    # Produce some messages
    producer.send("user-events", {"user_id": 1, "action": "login"}, key="user-1")
    producer.send("user-events", {"user_id": 2, "action": "signup"}, key="user-2")
    producer.send("user-events", {"user_id": 1, "action": "logout"}, key="user-1")
    producer.send("orders", {"order_id": 101, "amount": 99.99}, key="order-101")
    producer.send("orders", {"order_id": 102, "amount": 149.99}, key="order-102")
    
    # Produce batch
    batch_messages = [
        ("user-3", {"user_id": 3, "action": "login"}),
        ("user-4", {"user_id": 4, "action": "purchase"}),
        ("user-5", {"user_id": 5, "action": "logout"}),
    ]
    producer.send_batch("user-events", batch_messages)
    
    # Create consumers
    print("\n--- Consuming Messages ---")
    consumer1 = Consumer(broker, group_id="analytics-group")
    consumer1.subscribe(["user-events"])
    
    # Poll for messages
    messages = consumer1.poll(max_messages=5)
    print(f"\nConsumer1 received {len(messages)} messages:")
    for msg in messages:
        print(f"  Offset={msg.offset}, Partition={msg.partition}, Value={msg.value}")
    
    # Create another consumer in same group
    print("\n--- Second Consumer in Same Group ---")
    consumer2 = Consumer(broker, group_id="analytics-group")
    consumer2.subscribe(["user-events"])
    
    # This consumer should start from where the group left off
    messages = consumer2.poll(max_messages=5)
    print(f"\nConsumer2 received {len(messages)} messages:")
    for msg in messages:
        print(f"  Offset={msg.offset}, Partition={msg.partition}, Value={msg.value}")
    
    # Create consumer in different group
    print("\n--- Consumer in Different Group ---")
    consumer3 = Consumer(broker, group_id="monitoring-group")
    consumer3.subscribe(["user-events"])
    
    # This consumer should read from beginning
    messages = consumer3.poll(max_messages=10)
    print(f"\nConsumer3 received {len(messages)} messages:")
    for msg in messages:
        print(f"  Offset={msg.offset}, Partition={msg.partition}, Value={msg.value}")
    
    print("\n" + "=" * 70)
    print("Demo completed!")
    print("=" * 70)


if __name__ == "__main__":
    demo()
