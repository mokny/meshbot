# MeshBot - A full features Meshtastic Bot

This bot features...
- chat commands
- custom commands defined in config.toml
- fixed-time scheduled messages
- optional heartbeat messages 
- webhook forwarding
- SQLite persistence (stations, name history, last N messages per conversation)
- HTTP API with token authentication for sending messages and reading stats/history
- extendable via plugin system

## Requirements
- Docker
- A Meshtastic node that is connected to the same network as the machine the bot should run on

## Installation
Download the latest release and extract it to a dedicated directory.

## Configuration
Edit `config/config.toml`.

## Run
```bash
docker compose build --no-cache
docker compose up -d
```

## Viewing the logs
```bash
docker compose logs -f
```

## API Authentication
Use either:
- `Authorization: Bearer <token>`
- `X-Api-Token: <token>`

