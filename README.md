# Homework: Working with Kafka

This homework is designed to enhance your skills in working with Apache Kafka. The task involves creating and managing Kafka topics, producing and consuming messages, and implementing alert mechanisms for out-of-bound values.

The homework consists of the following files:

---

## 1. `create_topic.py`
This script is responsible for creating three Kafka topics:
- **`building_sensors_{my_name}`**: Used to store data from various sensors.
- **`temperature_alerts_{my_name}`**: Used to store temperature alert messages.
- **`humidity_alerts_{my_name}`**: Used to store humidity alert messages.

---

## 2. `sensor_producer.py`
This script simulates sensors by generating temperature and humidity values. These values are sent as messages to the **`building_sensors_{my_name}`** topic. The script should:
- Generate random temperature values (e.g., between 25°C and 45°C).
- Generate random humidity values (e.g., between 15% and 85%).

---

## 3. `sensor_consumer_producer.py`
This script:
- Subscribes to the **`building_sensors_{my_name}`** topic to consume sensor data.
- Processes the received values and checks for out-of-bound conditions:
  - **Temperature Alerts**: If the temperature exceeds the predefined threshold, an alert message is sent to **`temperature_alerts_{my_name}`**.
  - **Humidity Alerts**: If the humidity is outside the acceptable range, an alert message is sent to **`humidity_alerts_{my_name}`**.

The alert message should include the sensor ID, the value, and a description of the issue.

---

## 4. `alert_consumer.py`
This script subscribes to the alert topics:
- **`temperature_alerts_{my_name}`**
- **`humidity_alerts_{my_name}`**

It consumes alert messages from these topics and displays them in the console.

---
## 5. Result

[result_screen] [./img/task.PNG]
