# Rollback URL Tagging

Utility to remove `tag`/`tagging` query params from bulk archives, partner POS attachments, and client attachment URLs in the Lion Parcel database.

## Prerequisites

- Go 1.21+ (tested with 1.24.3)
- Access to the target MySQL database

## Setup

1. Create `.env` in the project root (or copy from an internal example if you have one).
2. Fill in `DB_DSN` with the correct MySQL DSN (e.g. `user:pass@tcp(host:3306)/db?parseTime=true`).
3. Adjust `BATCH_SIZE`, `DRY_RUN`, `HYDRA_SIGN_PREFIX`, and `BULK_S3_PREFIX` if needed.

Example `.env`:
```dotenv
DB_DSN=user:pass@tcp(localhost:3306)/lion?parseTime=true
BATCH_SIZE=200
DRY_RUN=1

# Hydra signed-URL prefix used for client attachment URLs
HYDRA_SIGN_PREFIX=https://api.dev-genesis.lionparcel.com/hydra/v1/asset/sign?

# S3 prefix used when normalizing bulk.archive_file URLs
# Dev:
BULK_S3_PREFIX=https://dev-genesis.s3.ap-southeast-1.amazonaws.com/
# Prod:
# BULK_S3_PREFIX=https://genesis.s3.ap-southeast-1.amazonaws.com/
```

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
