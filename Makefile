.PHONY: up build daemon down clean logs

MONGO_CONTAINER=mongo
MONGO_USER=admin
MONGO_PASS=admin123
MONGO_AUTH_DB=admin
MONGO_BACKUP_DIR=./mongo_backup
MONGO_DUMP_PATH=/data/dump

# List of databases to export
MONGO_DBS=

build:
	docker-compose build

up:
	docker-compose up

daemon:
	docker-compose up -d

down:
	docker-compose down --remove-orphans

clean:
	docker-compose down --volumes --remove-orphans
	rm -rf airflow/logs airflow/db mongo_data

logs:
	docker-compose logs -f

mongo-export:
	@mkdir -p $(MONGO_BACKUP_DIR)
	@for DB in $(MONGO_DBS); do \
		echo "📦 Dumping $$DB..."; \
		docker exec $(MONGO_CONTAINER) mongodump \
			--username $(MONGO_USER) \
			--password $(MONGO_PASS) \
			--authenticationDatabase $(MONGO_AUTH_DB) \
			--db $$DB \
			--out $(MONGO_DUMP_PATH); \
		docker cp $(MONGO_CONTAINER):$(MONGO_DUMP_PATH)/$$DB $(MONGO_BACKUP_DIR)/$$DB; \
	done
	@echo "✅ Export complete: $(MONGO_BACKUP_DIR)/"

mongo-import:
	@for DB in $(MONGO_DBS); do \
		echo "♻️  Restoring $$DB..."; \
		docker cp $(MONGO_BACKUP_DIR)/$$DB $(MONGO_CONTAINER):$(MONGO_DUMP_PATH)/$$DB; \
		docker exec $(MONGO_CONTAINER) mongorestore \
			--username $(MONGO_USER) \
			--password $(MONGO_PASS) \
			--authenticationDatabase $(MONGO_AUTH_DB) \
			--db $$DB \
			--drop \
			$(MONGO_DUMP_PATH)/$$DB; \
	done
	@echo "✅ Restore complete."

mongo-restore-full:
	@echo "🚀 Creating dump path inside container..."
	docker exec $(MONGO_CONTAINER) mkdir -p $(MONGO_DUMP_PATH)
	@echo "📦 Copying all backup files to container..."
	docker cp $(MONGO_BACKUP_DIR)/. $(MONGO_CONTAINER):$(MONGO_DUMP_PATH)
	@echo "♻️  Restoring full MongoDB dump..."
	docker exec $(MONGO_CONTAINER) mongorestore \
		--username $(MONGO_USER) \
		--password $(MONGO_PASS) \
		--authenticationDatabase $(MONGO_AUTH_DB) \
		--drop \
		$(MONGO_DUMP_PATH)
	@echo "✅ Full restore complete."

mongo-clean-backup:
	rm -rf $(MONGO_BACKUP_DIR)
	@echo "🗑️  Removed backup folder: $(MONGO_BACKUP_DIR)"
