.PHONY: help build tiles up down test logs shell deploy infra download-data clean

IMAGE := overture-tiles
SERVICE_IMAGE := ghcr.io/valhalla/valhalla:latest
DATA_DIR := $(shell pwd)/data

help:
	@echo "Available commands:"
	@echo "  make build          - Build the tile builder Docker image"
	@echo "  make tiles          - Build routing tiles from Overture data"
	@echo "  make up             - Start the Valhalla routing service"
	@echo "  make down           - Stop the routing service"
	@echo "  make test           - Run routing tests (needs make up)"
	@echo "  make logs           - View routing service logs"
	@echo "  make shell          - Shell into routing service container"
	@echo "  make download-data  - Download fresh Copenhagen Overture data"
	@echo "  make deploy         - Deploy via Kamal"
	@echo "  make infra          - Apply Terraform infrastructure"
	@echo "  make clean          - Remove tiles and stop containers"

# Build the tile builder image
build:
	docker build -t $(IMAGE) .

# Build routing tiles from Overture data (pedestrian-only)
tiles: build
	docker run --rm -v $(DATA_DIR):/data $(IMAGE) \
		/data/valhalla.json /data/connectors.parquet /data/segments.parquet --pedestrian-only

# Start the Valhalla routing service
up:
	@docker rm -f valhalla-service 2>/dev/null || true
	docker run -d --name valhalla-service \
		-p 8002:8002 \
		-v $(DATA_DIR):/data \
		$(SERVICE_IMAGE) valhalla_service /data/valhalla.json
	@echo "Routing service running at http://localhost:8002"

# Stop the routing service
down:
	@docker rm -f valhalla-service 2>/dev/null || true
	@echo "Stopped."

# Test a pedestrian route across Copenhagen
test:
	uv run pytest tests/ -v

# View routing service logs
logs:
	docker logs -f valhalla-service

# Shell into routing service
shell:
	docker exec -it valhalla-service bash

# Download fresh Overture data for Copenhagen
download-data:
	uv run python scripts/download_cph.py

# Deploy via Kamal
deploy:
	kamal deploy

# Apply infrastructure
infra:
	cd infra && terraform apply

# Clean up tiles and containers
clean: down
	sudo rm -rf $(DATA_DIR)/valhalla_tiles
