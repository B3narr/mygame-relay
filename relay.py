"""Chancebound Tales relay: lets friends play online without opening any
ports on their routers. Everyone connects *out* to this relay, and it passes
the game's messages between them. It never looks inside the messages: the
host's game still runs all the rules.

How it works (WebSocket paths):
  /host          a host opens a room and gets a room code: {"t": "room", "code": "K7QM2X"}
                 then sends {"t": "to", "peer": id, "data": "..."} or {"t": "all", "data": "..."}
                 and receives {"t": "joined", "peer": id}, {"t": "from", "peer": id, "data": "..."},
                 {"t": "left", "peer": id}
  /join/CODE     a player joins a room; after that they send and receive the game's
                 own messages as they are (the relay wraps and unwraps them)
  /              a plain web page saying the relay is running (for health checks)

Run it:  pip install -r requirements.txt   then   python relay.py
It listens on the port in the PORT environment variable (default 8765).
Set RELAY_TEST_CODES=1 to let hosts pick their own code (/host/CODE): only
for automated tests on your own computer.
"""
import asyncio
import json
import os
import secrets
import time

from websockets.asyncio.server import serve
from websockets.datastructures import Headers
from websockets.http11 import Response

CODE_LETTERS = "ABCDEFGHJKMNPQRSTVWXYZ23456789"  # no look-alikes (I, L, O, U, 0, 1)
CODE_LENGTH = 6
MAX_ROOMS = 500
MAX_PLAYERS = 8           # joiners per room
MAX_MESSAGE = 8 << 20     # 8 MB: a whole saved game can be sent when someone joins mid-game
FIRST_PEER_ID = 1000      # ids the relay gives players (the host's own game uses lower ones)
TEST_CODES = os.environ.get("RELAY_TEST_CODES") == "1"


class Room:
    def __init__(self, code, host):
        self.code = code
        self.host = host
        self.players = {}  # peer id -> websocket
        self.next_id = FIRST_PEER_ID
        self.created = time.time()


rooms = {}


def new_code():
    while True:
        code = "".join(secrets.choice(CODE_LETTERS) for _ in range(CODE_LENGTH))
        if code not in rooms:
            return code


async def send(websocket, message):
    try:
        await websocket.send(message if isinstance(message, str) else json.dumps(message))
    except Exception:
        pass  # they already left


async def host_room(websocket, wanted_code):
    if len(rooms) >= MAX_ROOMS:
        await send(websocket, {"t": "relay_full"})
        return
    code = wanted_code if (TEST_CODES and wanted_code and wanted_code not in rooms) else new_code()
    room = Room(code, websocket)
    rooms[code] = room
    await send(websocket, {"t": "room", "code": code})
    try:
        async for raw in websocket:
            try:
                message = json.loads(raw)
            except ValueError:
                continue
            kind = message.get("t")
            data = message.get("data", "")
            if not isinstance(data, str):
                continue
            if kind == "to":
                player = room.players.get(message.get("peer"))
                if player is not None:
                    await send(player, data)
            elif kind == "all":
                await asyncio.gather(*(send(p, data) for p in list(room.players.values())))
            elif kind == "kick":
                player = room.players.get(message.get("peer"))
                if player is not None:
                    await player.close()
    finally:
        rooms.pop(code, None)
        closing = json.dumps({"t": "closed"})
        for player in list(room.players.values()):
            await send(player, closing)
            await player.close()


async def join_room(websocket, code):
    room = rooms.get(code.upper())
    if room is None:
        await send(websocket, {"t": "no_party"})
        return
    if len(room.players) >= MAX_PLAYERS:
        await send(websocket, {"t": "party_full"})
        return
    peer = room.next_id
    room.next_id += 1
    room.players[peer] = websocket
    await send(room.host, {"t": "joined", "peer": peer})
    try:
        async for raw in websocket:
            if isinstance(raw, str):
                await send(room.host, {"t": "from", "peer": peer, "data": raw})
    finally:
        room.players.pop(peer, None)
        if rooms.get(room.code) is room:
            await send(room.host, {"t": "left", "peer": peer})


async def handler(websocket):
    path = websocket.request.path.split("?")[0].rstrip("/")
    parts = [p for p in path.split("/") if p]
    if parts[:1] == ["host"]:
        await host_room(websocket, parts[1].upper() if len(parts) > 1 else "")
    elif parts[:1] == ["join"] and len(parts) == 2:
        await join_room(websocket, parts[1])
    else:
        await websocket.close(1008, "unknown path")


def health_check(connection, request):
    """Plain web requests (not WebSockets) get a short 'I'm alive' page."""
    if request.headers.get("Upgrade", "").lower() != "websocket":
        body = ("Chancebound Tales relay is running. Rooms open: %d\n" % len(rooms)).encode()
        return Response(200, "OK", Headers([("Content-Type", "text/plain"), ("Content-Length", str(len(body)))]), body)
    return None


async def main():
    port = int(os.environ.get("PORT", "8765"))
    async with serve(handler, "0.0.0.0", port, max_size=MAX_MESSAGE, process_request=health_check,
                     ping_interval=20, ping_timeout=40):
        print("Chancebound Tales relay listening on port %d" % port, flush=True)
        await asyncio.get_running_loop().create_future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
