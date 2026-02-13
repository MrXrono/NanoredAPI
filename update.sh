#!/bin/bash
set -e

# ===== NanoredAPI Update Script =====
# Checks GitHub for updates, pulls changes, handles DB schema changes,
# rebuilds containers, and reloads nginx.

INSTALL_DIR="/opt/nanored-api"
REPO_URL="https://github.com/MrXrono/NanoredAPI.git"
COMPOSE_FILE="$INSTALL_DIR/docker-compose.yml"
NGINX_CONTAINER="remnawave-nginx"
HEALTH_URL="http://localhost:8000/health"
API_CONTAINER="nanored-api"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

FORCE=false
if [ "$1" = "--f" ] || [ "$1" = "--force" ]; then
    FORCE=true
fi

log() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')] ВНИМАНИЕ:${NC} $1"; }
err() { echo -e "${RED}[$(date '+%H:%M:%S')] ОШИБКА:${NC} $1"; }

# ---------- 1. Check current version ----------
log "Проверка текущей версии..."
if [ "$FORCE" = true ]; then
    log "Режим принудительного обновления (--f)"
fi
if [ ! -d "$INSTALL_DIR/.git" ]; then
    warn "Директория $INSTALL_DIR не является git-репозиторием"
    log "Инициализация git в существующей директории..."
    cd "$INSTALL_DIR"
    git init
    git remote add origin "$REPO_URL" 2>/dev/null || git remote set-url origin "$REPO_URL"
    git fetch origin main --quiet 2>/dev/null || git fetch origin master --quiet 2>/dev/null
    BRANCH=$(git remote show origin 2>/dev/null | grep 'HEAD branch' | awk '{print $NF}')
    BRANCH=${BRANCH:-main}
    git checkout -b "$BRANCH" 2>/dev/null || true
    git reset --mixed "origin/$BRANCH" 2>/dev/null || true
    log "Git инициализирован (ветка: $BRANCH)"
fi

cd "$INSTALL_DIR"
LOCAL_COMMIT=$(git rev-parse HEAD)
git fetch origin main --quiet 2>/dev/null || git fetch origin master --quiet 2>/dev/null
REMOTE_COMMIT=$(git rev-parse FETCH_HEAD 2>/dev/null)

if [ "$LOCAL_COMMIT" = "$REMOTE_COMMIT" ] && [ "$FORCE" = false ]; then
    log "Версия актуальна (${LOCAL_COMMIT:0:8})"
    log "Используйте --f для принудительного обновления."
    exit 0
fi

if [ "$LOCAL_COMMIT" = "$REMOTE_COMMIT" ] && [ "$FORCE" = true ]; then
    warn "Версия актуальна, но выполняется принудительное обновление"
fi

LOCAL_VER=$(grep -oP 'VERSION.*?=.*?"(\K[^"]+)' app/core/config.py 2>/dev/null || echo "unknown")
log "Текущая версия: $LOCAL_VER (commit: ${LOCAL_COMMIT:0:8})"
log "Доступно обновление: commit ${REMOTE_COMMIT:0:8}"

# ---------- 2. Save current models hash for schema comparison ----------
MODELS_HASH_BEFORE=""
if [ -d "app/models" ]; then
    MODELS_HASH_BEFORE=$(find app/models -name "*.py" -exec md5sum {} \; | sort | md5sum | cut -d' ' -f1)
fi

# ---------- 3. Pull updates ----------
log "Загрузка обновлений..."
if [ "$FORCE" = true ]; then
    BRANCH=$(git remote show origin 2>/dev/null | grep 'HEAD branch' | awk '{print $NF}')
    BRANCH=${BRANCH:-main}
    git reset --hard "origin/$BRANCH" 2>/dev/null
    git clean -fd 2>/dev/null || true
else
    git stash --quiet 2>/dev/null || true
    git pull origin main --quiet 2>/dev/null || git pull origin master --quiet 2>/dev/null
    git stash pop --quiet 2>/dev/null || true
fi

NEW_VER=$(grep -oP 'VERSION.*?=.*?"(\K[^"]+)' app/core/config.py 2>/dev/null || echo "unknown")
log "Новая версия: $NEW_VER"

# ---------- 4. Check DB schema changes ----------
MODELS_HASH_AFTER=""
if [ -d "app/models" ]; then
    MODELS_HASH_AFTER=$(find app/models -name "*.py" -exec md5sum {} \; | sort | md5sum | cut -d' ' -f1)
fi

SCHEMA_CHANGED=false
if [ "$MODELS_HASH_BEFORE" != "$MODELS_HASH_AFTER" ]; then
    SCHEMA_CHANGED=true
    warn "Обнаружены изменения в схеме БД!"
    warn "Модели изменились: $MODELS_HASH_BEFORE -> $MODELS_HASH_AFTER"
fi

# ---------- 5. Stop containers ----------
log "Остановка контейнеров..."
docker compose -f "$COMPOSE_FILE" down 2>/dev/null || docker-compose -f "$COMPOSE_FILE" down 2>/dev/null

# ---------- 6. Handle DB schema changes ----------
if [ "$SCHEMA_CHANGED" = true ]; then
    warn "Удаление старой БД из-за изменений схемы..."
    VOLUME_NAME=$(docker volume ls -q | grep nanored.*pgdata 2>/dev/null || true)
    if [ -n "$VOLUME_NAME" ]; then
        docker volume rm "$VOLUME_NAME" 2>/dev/null || true
        log "Том $VOLUME_NAME удалён"
    else
        # Try to find by compose project name
        docker volume rm nanored-api_nanored-pgdata 2>/dev/null || true
        docker volume rm nanored_nanored-pgdata 2>/dev/null || true
        log "Тома БД удалены (если существовали)"
    fi
fi

# ---------- 7. Rebuild and start ----------
log "Пересборка контейнеров..."
docker compose -f "$COMPOSE_FILE" build --no-cache 2>/dev/null || docker-compose -f "$COMPOSE_FILE" build --no-cache 2>/dev/null
log "Запуск контейнеров..."
docker compose -f "$COMPOSE_FILE" up -d 2>/dev/null || docker-compose -f "$COMPOSE_FILE" up -d 2>/dev/null

# ---------- 8. Wait for API to start ----------
log "Ожидание запуска API..."
for i in $(seq 1 30); do
    RESP=$(docker exec "$API_CONTAINER" curl -sf "$HEALTH_URL" 2>/dev/null || echo "")
    if echo "$RESP" | grep -q '"status"' 2>/dev/null; then
        break
    fi
    sleep 2
done

# ---------- 9. Reload nginx ----------
log "Перезагрузка nginx..."
docker exec "$NGINX_CONTAINER" nginx -s reload 2>/dev/null && log "Nginx перезагружен" || warn "Не удалось перезагрузить nginx ($NGINX_CONTAINER)"

# ---------- 10. Health check ----------
log "Проверка доступности..."
HEALTH_RESP=$(docker exec "$API_CONTAINER" curl -sf "$HEALTH_URL" 2>/dev/null || echo "")
if echo "$HEALTH_RESP" | grep -q '"status"'; then
    RUNNING_VER=$(echo "$HEALTH_RESP" | sed 's/.*"version":"\([^"]*\)".*/\1/' 2>/dev/null || echo "unknown")
    log "====================================="
    log "Обновление завершено успешно!"
    log "Версия API: $RUNNING_VER"
    if [ "$SCHEMA_CHANGED" = true ]; then
        warn "БД была пересоздана (схема изменилась)"
    fi
    log "====================================="
else
    err "API не отвечает!"
    err "Ответ: $HEALTH_RESP"
    err "Проверьте логи: docker compose -f $COMPOSE_FILE logs -f nanored-api"
    exit 1
fi
