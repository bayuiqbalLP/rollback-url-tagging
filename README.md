# Rollback URL Tagging

Utility to remove `tag`/`tagging` query params from bulk archives, partner POS attachments, and client attachment URLs in the Lion Parcel database.

## Prerequisites

- Go 1.21+ (tested with 1.24.3)
- Access to the target MySQL database

## Setup

1. Copy the example environment:
   ```sh
   cp .env.example .env
   ```
2. Fill in `DB_DSN` with the correct MySQL DSN (e.g. `user:pass@tcp(host:3306)/db?parseTime=true`).
3. Adjust `BATCH_SIZE`, `DRY_RUN`, and `HYDRA_SIGN_PREFIX` if needed.

## Running

```sh
# optional: avoid permission errors with local cache
export GOCACHE="${PWD}/.gocache"

go run main.go
```

- Keep `DRY_RUN=1` to inspect the planned changes without touching the database.
- Set `DRY_RUN=0` (or remove it) once you are confident with the output.

## Building

```sh
export GOCACHE="${PWD}/.gocache"
go build ./...
```

The resulting binary (`rollback-url-tagging`) respects the same env vars as above.***
