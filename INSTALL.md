# NanoredVPN Telemetry API — Установка и настройка

## Архитектура

```
Android Client (NanoredVPN)
        │
        │ HTTPS (X-API-Key)
        ▼
  ┌─────────────┐
  │   Nginx     │  api.nanored.top:443
  │  (reverse   │
  │   proxy)    │
  └──────┬──────┘
         │ :8000
  ┌──────▼──────┐
  │ FastAPI     │  nanored-api container
  │ (Backend)   │
  └──┬──────┬───┘
     │      │
┌────▼──┐ ┌─▼────────┐
│Postgre│ │   Redis   │
│  SQL  │ │  (cache)  │
└───────┘ └──────────┘
```

## Требования

- Docker + Docker Compose
- Существующий nginx контейнер (`remnawave-nginx`) в сети `remnawave-network`
- SSL сертификат для `api.nanored.top`

---

## 1. Клонирование репозитория

```bash
cd /opt
git clone https://github.com/MrXrono/NanoredAPI.git nanored-api
cd nanored-api
```

## 2. Настройка переменных окружения

```bash
cp .env.example .env
nano .env
```

Измените значения:
```
SECRET_KEY=ваш-случайный-ключ-минимум-32-символа
ADMIN_USERNAME=admin
ADMIN_PASSWORD=ваш-надёжный-пароль
TELEGRAM_MESSAGE_BOT_TOKEN=токен-телеграм-бота-для-саппорта
TELEGRAM_SUPPORT_GROUP_ID=-1002021945145
TELEGRAM_SUPPORT_DATA_FILE=data.json
```

Для генерации SECRET_KEY:
```bash
openssl rand -base64 32
```

## 3. GeoIP база (опционально)

Для определения геолокации клиентов по IP:

```bash
mkdir -p data
# Скачайте GeoLite2-City.mmdb с https://dev.maxmind.com/geoip/geolite2-free-geolocation-data
# Положите в data/GeoLite2-City.mmdb
```

## 4. SSL сертификат для api.nanored.top

Создайте сертификат (Let's Encrypt или ваш CA) и положите файлы:

```bash
# Пример расположения (рядом с другими сертификатами):
/opt/remnawave/nginx/api.nanored.top.fullchain.pem
/opt/remnawave/nginx/api.nanored.top.privkey.key
```

## 5. Настройка Nginx

### 5.1. Добавить volume для SSL в docker-compose nginx

В docker-compose файле nginx (`/opt/remnawave/docker-compose.yml`) добавьте volumes:

```yaml
services:
  remnawave-nginx:
    volumes:
      # ... существующие volumes ...
      - /opt/remnawave/nginx/api.nanored.top.fullchain.pem:/etc/nginx/ssl/api.nanored.top.fullchain.pem:ro
      - /opt/remnawave/nginx/api.nanored.top.privkey.key:/etc/nginx/ssl/api.nanored.top.privkey.key:ro
```

### 5.2. Добавить конфигурацию в nginx.conf

Скопируйте содержимое `nginx-api.conf` в основной `nginx.conf`:

```bash
# Добавьте upstream ПЕРЕД первым server блоком:
upstream nanored-api {
    server nanored-api:8000;
}

# Добавьте server блок в конец файла (содержимое nginx-api.conf)
```

Или вручную:
```bash
cat nginx-api.conf >> /путь/к/nginx.conf
```

**Важно:** upstream `nanored-api` должен быть добавлен в начало файла, рядом с другими upstream.

## 6. Запуск API

```bash
cd /opt/nanored-api

# Собрать и запустить
docker compose up -d --build

# Проверить статус
docker compose ps

# Посмотреть логи
docker compose logs -f nanored-api
```

## 6.1. Telegram webhook (обязательно для ответов/закрытия заявок)

Чтобы `NanoredAPI` получал ответы саппорта из тем и событие закрытия темы, Telegram-бот должен слать апдейты в вебхук:

```bash
# Вставьте токен бота и ваш публичный URL (должен быть доступен Telegram)
BOT_TOKEN="..."
WEBHOOK_URL="https://api.nanored.top/api/v1/client/support/telegram/webhook"

curl -sS "https://api.telegram.org/bot${BOT_TOKEN}/setWebhook" \\
  -d "url=${WEBHOOK_URL}" \\
  -d 'allowed_updates=["message","edited_message"]'
```

Проверить текущий вебхук:
```bash
curl -sS "https://api.telegram.org/bot${BOT_TOKEN}/getWebhookInfo"
```

## 7. Перезапуск Nginx

```bash
# Проверить конфигурацию
docker exec remnawave-nginx nginx -t

# Перезагрузить
docker exec remnawave-nginx nginx -s reload
```

## 8. Проверка работоспособности

```bash
# Health check
curl -k https://api.nanored.top/health

# Ожидаемый ответ:
# {"status":"ok","version":"1.0.0"}

# Тест авторизации
curl -k -X POST https://api.nanored.top/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"nanored_admin_2026"}'

# Ожидаемый ответ:
# {"access_token":"eyJ...","token_type":"bearer"}
```

## 9. Доступ к админ-панели

Откройте в браузере: **https://api.nanored.top**

Введите логин и пароль администратора.

---

## Интеграция в Android клиент (NanoredVPN)

### 1. Скопируйте файл SDK

Скопируйте `android-sdk/src/main/java/com/nanored/vpn/telemetry/NanoredTelemetry.kt`
в проект NanoredVPN: `app/src/main/java/com/nanored/vpn/telemetry/NanoredTelemetry.kt`

### 2. Инициализация в Application

```kotlin
// В классе Application или MainActivity.onCreate:
NanoredTelemetry.init(this, "https://api.nanored.top")
```

### 3. При подключении VPN

```kotlin
// Когда VPN подключается:
NanoredTelemetry.startSession(
    serverAddress = "server1.example.com",
    protocol = "VLESS"
)
```

### 4. Сбор телеметрии (во время работы VPN)

```kotlin
// SNI из TLS handshake:
NanoredTelemetry.addSNI("youtube.com", hitCount = 1, bytesTotal = 1024)

// DNS запросы:
NanoredTelemetry.addDNS("google.com", resolvedIp = "142.250.74.46")

// Трафик приложений:
NanoredTelemetry.addAppTraffic("com.google.android.youtube", "YouTube", bytesDown = 5000000, bytesUp = 100000)

// Соединения:
NanoredTelemetry.addConnection("142.250.74.46", 443, "TCP", "google.com")
```

Данные буферизируются и автоматически отправляются каждые 60 секунд.

### 5. При отключении VPN

```kotlin
NanoredTelemetry.endSession(
    bytesDownloaded = totalBytesDown,
    bytesUploaded = totalBytesUp,
    connectionCount = connCount,
    reconnectCount = reconnects
)
```

### 6. Отчёт об ошибках

```kotlin
NanoredTelemetry.reportError(
    errorType = "connection_failed",
    message = "Timeout connecting to server",
    stacktrace = exception.stackTraceToString()
)
```

---

## API Endpoints

### Клиентские (X-API-Key auth)

| Метод | Путь | Описание |
|-------|------|----------|
| POST | `/api/v1/client/register` | Регистрация устройства |
| POST | `/api/v1/client/session/start` | Начало VPN сессии |
| POST | `/api/v1/client/session/end` | Конец VPN сессии |
| POST | `/api/v1/client/session/heartbeat` | Heartbeat (keep-alive) |
| POST | `/api/v1/client/sni/batch` | Пакетная отправка SNI |
| POST | `/api/v1/client/dns/batch` | Пакетная отправка DNS |
| POST | `/api/v1/client/app-traffic/batch` | Трафик приложений |
| POST | `/api/v1/client/connections/batch` | IP соединения |
| POST | `/api/v1/client/error` | Отчёт об ошибке |

### Админские (Bearer JWT auth)

| Метод | Путь | Описание |
|-------|------|----------|
| POST | `/api/v1/auth/login` | Авторизация |
| GET | `/api/v1/admin/dashboard` | Дашборд |
| GET | `/api/v1/admin/devices` | Список устройств |
| GET | `/api/v1/admin/devices/{id}` | Детали устройства |
| POST | `/api/v1/admin/devices/{id}/block` | Блокировка |
| POST | `/api/v1/admin/devices/{id}/unblock` | Разблокировка |
| GET | `/api/v1/admin/sessions` | История сессий |
| GET | `/api/v1/admin/sni` | SNI логи |
| GET | `/api/v1/admin/sni/top` | Топ доменов |
| GET | `/api/v1/admin/dns` | DNS логи |
| GET | `/api/v1/admin/app-traffic` | Трафик приложений |
| GET | `/api/v1/admin/app-traffic/top` | Топ приложений |
| GET | `/api/v1/admin/connections` | IP соединения |
| GET | `/api/v1/admin/errors` | Ошибки |
| GET | `/api/v1/admin/export/sni` | Экспорт SNI в CSV |

Swagger документация: **https://api.nanored.top/docs**

---

## Обслуживание

### Бэкап БД

```bash
docker exec nanored-db pg_dump -U nanored nanored_api > backup_$(date +%Y%m%d).sql
```

### Восстановление БД

```bash
cat backup.sql | docker exec -i nanored-db psql -U nanored nanored_api
```

### Обновление

```bash
cd /opt/nanored-api
git pull
docker compose up -d --build
```

### Логи

```bash
docker compose logs -f nanored-api    # API логи
docker compose logs -f nanored-db     # PostgreSQL логи
docker compose logs -f nanored-redis  # Redis логи
```
