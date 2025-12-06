# Market Wide Anomaly Tracker

### Mission:

To detect stock moves that are considere statistical anomalies and then quickly determine the fundamental cause for the move in the stock market, providing traders with instant, context-rich alerts for unexpected, non-market-drive price movements.

## Core Components

# Anomaly Detection System 

- Ingests real-time pricing data from polygon.io maintaining a state for each tracked stock.
- Detects when an anomalous move occurs in a shares price, triggering an anomalous-move alert which triggers an alert to all connected clients & the triggering of an agentic system that aims to determine the reason for the move.
- Given the high number of shares that will be tracked this will likely need to be a distributed system that will require automatic deployment to a cloud provider.
  
# Anomaly Detection Client

- Local client that runs on the users local system and ingests anomalous-move alerts and the corresponding output from the agentic pipeline, displaying both to the user via the terminal.

# Agentic Move Cause Analysis Pipeline

- TBD: Some sort of agentic system that scans a set of datasources or the internet as a whole to attempt to determine the fundamental cause for an anomalous share price move.


  
