# 🤖 MeshBot - A full featured Meshtastic Bot

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
Edit the well documented `config/config.toml`. Make sure to set `commands_enabled` to `true`, if you want your bot to reply to commands. Otherwise there will be a lot of silence, as this option is disabled by default. Also make sure to change the API-Tokens, even if you do not use the API! (!!)

## Run
```bash
docker compose build --no-cache
docker compose up -d
```

## Viewing the logs
```bash
docker compose logs -f
```

## The bot has an API...
Yep it has and its configuration is stored in the `config/config.toml` file. Attention: Even if you do not use the API, change the authentication tokens in the configuration!
To access the bot API, you need to send one of the configured auth tokens inside your POST request header.
Use either:
- `Authorization: Bearer <token>`
- `X-Api-Token: <token>`

## Using plugins
Plugins are located in the `plugins` directory. Plugins are individual Python programs that receive events from the bot and that can access the main functions of the bot. Plugins are loaded in alphabetical order. Plugins whose filenames start with `_` (underscore), are disabled and will not be loaded. If you want to get into plugin coding, please refer to the included example plugin that can be used as a template.

# Changelog
### 2.01
- Added `commands_enabled` option. By default ALL commands are disabled. Set this option to `true` in the config.toml file to make your bot respond to commands.

### 2.0
- Initial release
