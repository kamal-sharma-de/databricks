# Install the websocket-client library (if not already installed)
# You can run this command only once in your Databricks cluster
# Remove the '#' before the 'pip install' command
#%pip install websocket-client

import websocket
import json
from pyspark.sql import Row

#Replace 'wss://example.com' with the actual WebSocket URL
websocket_url = 'wss://example.com'

parquet_output_path = 'dbfs:/coinbase/output'

def on_message(ws, message):
    data = json.loads(message)

    # Write the streaming data to Parquet format
    if data['type'] == 'ticker':
        raw_data = [Row(**data)]
        df = spark.createDataFrame(raw_data)
        df.write.parquet(parquet_output_path, mode='append')
        print("Saved")
    else:
        print(f"Received message: {data}")
        pass

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Closed WebSocket connection")

def on_open(ws):
    # Subscribe to a specific product channel (replace with your desired product)
    subscribe_message = {
        "type": "subscribe",
        "channels": [{"name": "ticker", "product_ids": ["BTC-USD","ETH-EUR","BTC-USD","USDT-USD","USDT-EUR","DOGE-USD","DOGE-EUR"
                                                        ,"SOL-USD","SOL-EUR","XRP-USD","XRP-EUR"]}]
    }
    ws.send(json.dumps(subscribe_message))
    print("WebSocket connection opened")

# Connect to the WebSocket
ws = websocket.WebSocketApp(websocket_url, on_message=on_message, on_error=on_error, on_close=on_close)
ws.on_open = on_open

# Keep the connection open (you can interrupt the execution when you're done)
ws.run_forever()
