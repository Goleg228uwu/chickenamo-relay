import asyncio
import json
import os
import websockets

ROOMS = {}          # имя комнаты -> данные комнаты
MAX_PLAYERS = 8     # максимум игроков в комнате
MAX_NAME = 10       # максимум символов в имени комнаты
HEARTBEAT = 20      # секунд между пингами (держит соединение живым)

async def send(ws, data):
    """Отправить словарь одному клиенту."""
    await ws.send(json.dumps(data, ensure_ascii=False))

async def broadcast(room, data, skip=None):
    """Отправить словарь всем в комнате, кроме skip."""
    for ws in list(room["peers"].values()):
        if ws is not skip:
            try:
                await send(ws, data)
            except Exception:
                pass

async def heartbeat(ws):
    """Пингует клиента, чтобы соединение не засыпало."""
    try:
        while True:
            await asyncio.sleep(HEARTBEAT)
            await ws.ping()
    except Exception:
        pass

async def handler(ws, path=None):
    room_name, pid = None, None
    hb = asyncio.create_task(heartbeat(ws))
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            t = msg.get("t")

            # --- Вход в комнату ---
            if room_name is None:
                if t == "host":
                    name = str(msg.get("room", "")).strip()[:MAX_NAME]
                    if name == "" or name in ROOMS:
                        await send(ws, {"t": "err", "code": "taken" if name in ROOMS else "badname"})
                        await ws.close()
                        return
                    ROOMS[name] = {
                        "peers": {1: ws},
                        "nicks": {1: str(msg.get("nick", ""))[:12]},
                        "next_id": 2,
                        "snap": {},
                    }
                    room_name, pid = name, 1
                    print(f"[+] Хост создал комнату '{name}' (id=1)")
                    await send(ws, {
                        "t": "welcome",
                        "id": 1,
                        "room": name,
                        "players": dict(ROOMS[name]["nicks"]),
                    })
                elif t == "join":
                    name = str(msg.get("room", "")).strip()
                    room = ROOMS.get(name)
                    if room is None:
                        await send(ws, {"t": "err", "code": "not_found"})
                        await ws.close()
                        return
                    if len(room["peers"]) >= MAX_PLAYERS:
                        await send(ws, {"t": "err", "code": "full"})
                        await ws.close()
                        return
                    pid = room["next_id"]
                    room["next_id"] += 1
                    room["peers"][pid] = ws
                    room["nicks"][pid] = str(msg.get("nick", ""))[:12]
                    room_name = name
                    print(f"[+] Игрок id={pid} зашёл в комнату '{name}'")
                    await send(ws, {
                        "t": "welcome",
                        "id": pid,
                        "room": name,
                        "players": dict(room["nicks"]),
                    })
                    # Отправляем новичку последние позиции всех игроков
                    for sid, snap in room["snap"].items():
                        if sid != pid:
                            try:
                                await send(ws, snap)
                            except Exception:
                                pass
                    await broadcast(room, {
                        "t": "joined",
                        "id": pid,
                        "nick": room["nicks"][pid],
                    }, skip=ws)
                else:
                    await ws.close()
                    return

            # --- Сообщения внутри комнаты ---
            else:
                room = ROOMS.get(room_name)
                if room is None:
                    return
                # Запоминаем позиции для снимка мира
                if msg.get("t") == "pos" and msg.get("id") == pid:
                    room["snap"][pid] = msg
                # Пересылаем сообщение всем остальным игрокам
                for other in list(room["peers"].values()):
                    if other is not ws:
                        try:
                            await other.send(raw)
                        except Exception:
                            pass
    except websockets.ConnectionClosed:
        pass
    finally:
        hb.cancel()
        if room_name is not None:
            room = ROOMS.get(room_name)
            if room:
                room["peers"].pop(pid, None)
                room["nicks"].pop(pid, None)
                room["snap"].pop(pid, None)
                if pid == 1:
                    ROOMS.pop(room_name, None)
                    print(f"[-] Комната '{room_name}' закрыта (хост вышел)")
                    await broadcast(room, {"t": "closed"})
                else:
                    print(f"[-] Игрок id={pid} вышел из комнаты '{room_name}'")
                    await broadcast(room, {"t": "left", "id": pid})

async def main():
    # Render даёт порт через переменную окружения PORT
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(handler, "0.0.0.0", port, max_size=2**22):
        print(f"Реле запущено на порту {port}")
        await asyncio.Future()

asyncio.run(main())
