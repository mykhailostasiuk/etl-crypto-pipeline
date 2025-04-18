import yaml
from itertools import product

class Config:
    def __init__(self, config_path):
        self.config_path = config_path
        with open(config_path, "r") as file:
            self.config_data = yaml.safe_load(file)

    def markets_list(self):
        markets = self.config_data.get("markets", [])
        if not markets:
            raise ValueError("Markets are not specified or empty")
        return ','.join([m.lower() for m in markets if m])

    def endpoints_list(self):
        endpoints = self.config_data.get("endpoints", [])
        if not endpoints:
            raise ValueError("Websocket endpoints are not specified or empty")
        return ','.join([endpoint for endpoint in endpoints if endpoint])

    def websocket_subscriptions(self):
        markets = self.markets_list().split(',')
        endpoints = self.endpoints_list().split(',')

        if not markets or not endpoints:
            raise ValueError("Markets or Kafka topics were not specified")

        return ','.join([f"{market}@{endpoint}" for market, endpoint in product(markets, endpoints)])
