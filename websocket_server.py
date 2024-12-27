# websocket_server.py
import logging
import asyncio
import websockets

class WebSocketServer:
    def __init__(self, websocket_port=3001):
        self.websocket_port = websocket_port
        self.connected_clients = set()
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('websocket_server.log'),
                logging.StreamHandler()
            ]
        )
    
    async def register_client(self, websocket):
        """Register a new WebSocket client"""
        self.connected_clients.add(websocket)
        logging.info(f"New client connected. Total clients: {len(self.connected_clients)}")

    async def unregister_client(self, websocket):
        """Unregister a WebSocket client"""
        self.connected_clients.remove(websocket)
        logging.info(f"Client disconnected. Total clients: {len(self.connected_clients)}")

    async def handle_websocket_connection(self, websocket, path=None):
        """Handle WebSocket connections"""
        await self.register_client(websocket)
        try:
            await websocket.wait_closed()
        finally:
            await self.unregister_client(websocket)

    async def start_server(self):
        """Start the WebSocket server"""
        async with websockets.serve(self.handle_websocket_connection, "0.0.0.0", self.websocket_port):
            logging.info(f"WebSocket server started on port {self.websocket_port}")
            await asyncio.Future()  # Run forever