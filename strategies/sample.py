
import json

class SampleStrategy:
    def __init__(self, params_json: str):
        self.params = json.loads(params_json)

    def on_tick(self):
        # Gib eine Liste von TradeIntents als JSON‑String zurück
        # Hier leer – reine Demo
        return json.dumps([])
