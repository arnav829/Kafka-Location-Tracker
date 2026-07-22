# 📍 Kafka Location Tracker

A real-time location tracking application built with **Apache Kafka**, **Python**, **Docker**, and **HTML/CSS/JavaScript**. The application streams live location updates through Kafka and displays them on a simple web dashboard, demonstrating an event-driven architecture.

---

## 🚀 Features

- 📍 Real-time location data streaming
- ⚡ Apache Kafka event processing
- 🐍 Python Producer & Consumer
- 🐳 Dockerized Kafka setup
- 🌐 Interactive web dashboard
- 📡 Event-driven architecture

---

## 🛠️ Tech Stack

- Apache Kafka
- Python
- Docker & Docker Compose
- HTML
- CSS
- JavaScript

---

## 📂 Project Structure

```text
Kafka-Location-Tracker/
│── producer.py          # Produces location data
│── consumer.py          # Consumes Kafka messages
│── index.html           # Frontend dashboard
│── docker-compose.yml   # Kafka setup
│── requirements.txt
│── README.md
```

---

## ⚙️ Installation

### Clone the repository

```bash
git clone https://github.com/your-username/Kafka-Location-Tracker.git
cd Kafka-Location-Tracker
```

### Create a virtual environment

```bash
python -m venv venv
```

### Activate the environment

**Windows**

```bash
venv\Scripts\activate
```

**Linux/macOS**

```bash
source venv/bin/activate
```

### Install dependencies

```bash
pip install -r requirements.txt
```

---

## 🐳 Start Kafka

```bash
docker compose up -d
```

Verify the container is running:

```bash
docker ps
```

---

## ▶️ Run the Application

Start the consumer:

```bash
python consumer.py
```

Start the producer:

```bash
python producer.py
```

Open `index.html` in your browser to view the location updates.

---

## 📖 Project Workflow

```text
Producer
    │
    ▼
Apache Kafka
    │
    ▼
Consumer
    │
    ▼
Frontend Dashboard
```

---

## 📌 Future Improvements

- 🗺️ Google Maps integration
- 📍 Multiple device tracking
- ☁️ Cloud deployment
- 📊 Analytics dashboard
- 🔔 Alerts and notifications

---

## 👨‍💻 Author

**Arnav Gupta**

⭐ If you like this project, consider giving it a star on GitHub!
