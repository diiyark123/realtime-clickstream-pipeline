.PHONY: up down reset logs ps topic psql metrics quarantine lake test lint demo check

up:            ## Build and start the whole pipeline
	docker compose up -d --build

down:          ## Stop containers (keeps data)
	docker compose down

reset:         ## Stop and wipe everything: volumes, lake, checkpoints
	docker compose down -v
	rm -rf data/lake data/checkpoints

logs:          ## Follow producer + spark logs
	docker compose logs -f producer spark

ps:
	docker compose ps

topic:         ## Peek at 5 raw messages on the topic
	docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
		--topic clickstream_events --from-beginning --max-messages 5

psql:          ## Open a psql shell
	docker exec -it postgres psql -U pipeline -d clickstream

metrics:       ## Latest per-minute revenue
	docker exec postgres psql -U pipeline -d clickstream -c \
		"SELECT * FROM v_revenue_per_minute ORDER BY window_start DESC LIMIT 10;"

quarantine:    ## Data quality summary
	docker exec postgres psql -U pipeline -d clickstream -c \
		"SELECT error_reason, COUNT(*) FROM quarantine_events GROUP BY 1 ORDER BY 2 DESC;"

lake:          ## Query the Parquet lake with DuckDB
	python scripts/inspect_lake.py data/lake/events

test:
	pytest -v

lint:
	ruff check .

demo:          ## Guided demo: send a test purchase and check every stage
	@bash scripts/demo_flow.sh

check:         ## One-screen health report of every stage
	@bash scripts/check_pipeline.sh
