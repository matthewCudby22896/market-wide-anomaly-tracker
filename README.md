# 🚨 Project: Market-Wide Anomaly Tracker (MWAT)

## Mission:
To provide traders with instant, context-rich alerts for unexpected, non-market-driven price movements. The MWAT system detects stock moves that are statistical anomalies and immediately determines the fundamental cause, transforming raw market noise into actionable intelligence.

---

## Core Components

### 1. Anomaly Detection Engine

This component is the system's core quantitative brain, responsible for high-speed data ingestion and anomaly identification.

* **Real-Time Data Ingestion:** Ingests live pricing data from Polygon.io, maintaining a continuous state for every tracked stock across the market.
* **Anomaly Detection:** Continuously compares a stock's current price change against its historical and expected behavior. When an anomalous move is detected, it triggers a system-wide alert and initiates the Agentic Analysis Pipeline.
* **Scalable Architecture:** Given the high volume and velocity of data required to track the entire market, the system is designed as a distributed application, requiring automatic deployment and scaling on a cloud provider.

---

### 2. Agentic Cause Analysis Pipeline

This intelligent system is triggered by an anomaly alert, transforming from a passive monitor into an active research analyst within seconds.

* **Objective:** To swiftly scan and process a wide range of external data sources (including news sites and social media) to determine the **fundamental cause** for the share price move.
* **Process:** The pipeline employs intelligent agents to aggregate, filter, and analyze information related to the anomalous ticker and time window.
* **Output:** Generates a concise summary and classification of the likely cause (e.g., earnings, FDA approval, rumor), which is then packaged with the original alert.

---

### 3. Local Client Interface

The user-facing component designed for speed and clarity.

* **Alert Delivery:** A lightweight local application that runs on the user's system, maintaining a persistent connection to the MWAT backend.
* **User Interface:** Displays both the raw **Anomalous-Move Alert** (e.g., Ticker, magnitude, time) and the corresponding **Agentic Analysis Output** (summary and cause) in a clear, terminal-based interface.
