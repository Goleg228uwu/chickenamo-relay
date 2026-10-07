import asyncio, json
import websockets

ROOMS = {}
MAX_PLAYERS = 8
MAX_NAME = 10

async def send(ws, data):
    await ws.send(json.dumps(data, ensure_ascii=False))

async def broadcast(room, data, skip=None):
    for ws in list(room["peers"].values()):
        if ws is not skip:
            try: await send(ws, data)
            except Exception: pass

async def handler(ws, path=None):
    room_name, pid = None, None
    try:
        async for raw in ws:
            try: msg = json.loads(raw)
            except Exception: continue
            t = msg.get("t")

            if room_name is None:
                if t == "host":
                    name = str(msg.get("room", "")).strip()[:MAX_NAME]
                    if name == "" or name in ROOMS:
                        await send(ws, {"t": "err", "code": "taken" if name in ROOMS else "badname"})
                        await ws.close(); return
                    ROOMS[name] = {"peers": {1: ws}, "nicks": {1: str(msg.get("nick",""))[:12]}, "next_id": 2}
                    room_name, pid = name, 1
                    await send(ws, {"t": "welcome", "id": 1, "room": name, "players": dict(ROOMS[name]["nicks"])})
                elif t == "join":
                    name = str(msg.get("room", "")).strip()
                    room = ROOMS.get(name)
                    if room is None:
                        await send(ws, {"t": "err", "code": "not_found"}); await ws.close(); return
                    if len(room["peers"]) >= MAX_PLAYERS:
                        await send(ws, {"t": "err", "code": "full"}); await ws.close(); return
                    pid = room["next_id"]; room["next_id"] += 1
                    room["peers"][pid] = ws
                    room["nicks"][pid] = str(msg.get("nick",""))[:12]
                    room_name = name
                    await send(ws, {"t": "welcome", "id": pid, "room": name, "players": dict(room["nicks"])})
                    await broadcast(room, {"t": "joined", "id": pid, "nick": room["nicks"][pid]}, skip=ws)
                else:
                    await ws.close(); return
            else:
                room = ROOMS.get(room_name)
                if room is None: return
                dst = msg.get("dst")
                if dst is not None:
                    target = room["peers"].get(int(dst))
                    if target is not None and target is not ws:
                        try: await target.send(raw)
                        except Exception: pass
                else:
                    # Пересылаем сообщение всем остальным игрокам
                    for other in list(room["peers"].values()):
                        if other is not ws:
                            try: await other.send(raw)
                            except Exception: pass
    except websockets.ConnectionClosed:
        pass
    finally:
        if room_name is not None:
            room = ROOMS.get(room_name)
            if room:
                room["peers"].pop(pid, None); room["nicks"].pop(pid, None)
                if pid == 1:
                    ROOMS.pop(room_name, None)
                    await broadcast(room, {"t": "closed"})
                else:
                    await broadcast(room, {"t": "left", "id": pid})

async def main():
    async with websockets.serve(handler, "0.0.0.0", 8765, max_size=2**22):
        print("Реле запущено на порту 8765")
        await asyncio.Future()

asyncio.run(main())