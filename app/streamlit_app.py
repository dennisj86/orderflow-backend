import streamlit as st
from kafka import KafkaConsumer
import json

st.set_page_config(page_title="BTC-USDT Trades", layout="wide")

consumer = KafkaConsumer(
    "binance-trades-spot",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

st.title("📈 Binance Live Trades - BTC/USDT")

placeholder = st.empty()

while True:
    for message in consumer:
        trade = message.value
        placeholder.markdown(f"""
        **Price:** {trade['data']['price']}  
        **Amount:** {trade['data']['amount']}  
        **Side:** {trade['data']['side']}  
        **Time:** {trade['data']['timestamp']}
        """)
