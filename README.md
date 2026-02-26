# MeshBot - A full featured Meshtastic Bot

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
```bash
git clone https://github.com/mokny/meshbot
cd meshbot
```

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

# Changelog
###2.01
- Added `commands_enabled` option. By default ALL commands are disabled. Set this option to `true` in the config.toml file to make your bot respond to commands.
###2.0
- Initial release
