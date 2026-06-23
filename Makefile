# Convenience targets. On Windows, run the underlying commands directly if you
# don't have `make` (see README).

.PHONY: proto test up down logs clean

proto:          ## generate gRPC stubs from proto/gfs.proto
	uv run python scripts/gen_proto.py

test: proto     ## run the end-to-end tests in-process
	uv run pytest

up:             ## build images and start the cluster
	docker compose up --build -d

down:           ## stop the cluster
	docker compose down

logs:           ## follow naming + storage logs
	docker compose logs -f naming storage1 storage2 storage3

clean: down     ## stop the cluster and remove data volumes
	docker compose down -v
