import asyncio
import json
import os

import websockets

ROOMS = {}
NEXT_ID = [0]


async def handler(ws, path=None):
    pid = None
    room_name = None
    try:
        async for raw in ws:
            try:
                data = json.loads(raw)
            except Exception:
                continue
            t = data.get("t")

            if room_name is None:
                if t == "host":
                    name = str(data.get("room", ""))[:10]
                    if name in ROOMS:
                        await ws.send(json.dumps({"t": "err", "code": "taken"}))
                        await ws.close()
                        return
                    NEXT_ID[0] += 1
                    pid = NEXT_ID[0]
                    ROOMS[name] = {"peers": {pid: ws}, "nicks": {pid: str(data.get("nick", ""))[:12]}}
                    room_name = name
                    await ws.send(json.dumps({"t": "welcome", "id": pid, "room": name, "players": ROOMS[name]["nicks"]}))
                    print("[+] host '%s' id=%d" % (name, pid), flush=True)
                elif t == "join":
                    name = str(data.get("room", ""))[:10]
                    if name not in ROOMS:
                        await ws.send(json.dumps({"t": "err", "code": "not_found"}))
                        await ws.close()
                        return
                    room = ROOMS[name]
                    if len(room["peers"]) >= 8:
                        await ws.send(json.dumps({"t": "err", "code": "full"}))
                        await ws.close()
                        return
                    NEXT_ID[0] += 1
                    pid = NEXT_ID[0]
                    room["peers"][pid] = ws
                    room["nicks"][pid] = str(data.get("nick", ""))[:12]
                    room_name = name
                    await ws.send(json.dumps({"t": "welcome", "id": pid, "room": name, "players": room["nicks"]}))
                    msg = json.dumps({"t": "joined", "id": pid, "nick": room["nicks"][pid]})
                    for other in list(room["peers"].values()):
                        if other is not ws:
                            await other.send(msg)
                    print("[+] join '%s' id=%d" % (name, pid), flush=True)
                else:
                    await ws.close()
                    return
            else:
                room = ROOMS.get(room_name)
                if room is None:
                    await ws.close()
                    return
                for other in list(room["peers"].values()):
                    if other is not ws:
                        try:
                            await other.send(raw)
                        except Exception:
                            pass

    except websockets.ConnectionClosed:
        pass
    except Exception as e:
        print("[-] error:", e, flush=True)
    finally:
        room = ROOMS.get(room_name) if room_name else None
        if room is not None and pid in room["peers"]:
            del room["peers"][pid]
            room["nicks"].pop(pid, None)
            left = json.dumps({"t": "left", "id": pid})
            for other in list(room["peers"].values()):
                try:
                    await other.send(left)
                except Exception:
                    pass
            if not room["peers"]:
                ROOMS.pop(room_name, None)
                print("[-] room '%s' removed" % room_name, flush=True)


async def main():
    port = int(os.environ.get("PORT", 8080))
    async with websockets.serve(handler, "0.0.0.0", port, max_size=2 ** 22):
        print("relay on port %d" % port, flush=True)
        await asyncio.Future()


asyncio.run(main())
