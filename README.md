# VoD Training Platform - Backend

FastAPI API + MongoDB + Redis cache + origin health checker, run with Docker Compose.
The Next.js player (`src/app/watch/[itemId]/page.jsx`) is deployed separately with your frontend.

## 1. Start the stack

```bash
cp .env.example .env
# Fill in every empty value. Generate secrets with:  openssl rand -hex 32
docker compose up -d --build
docker compose ps          # all services should become "healthy"/"running"
curl http://127.0.0.1:8000/healthz
```

Compose refuses to start if a required secret is missing. The API listens on `127.0.0.1:8000`
only: put a TLS-terminating reverse proxy (Caddy, nginx, cloud load balancer) in front of it.
MongoDB and Redis are on an internal network and are never published to the host.

The first start creates a limited MongoDB user from `MONGO_APP_USER` / `MONGO_APP_PASSWORD`.
That script only runs on an empty data volume; changing those values later means updating the
user in MongoDB (or recreating the volume if you have no data yet).

## 2. The health checker (background worker)

It's the `health-checker` service in the compose file: it starts with the stack, restarts on
failure, and needs no extra setup.

```bash
docker compose logs -f health-checker                              # watch cycles / transitions
docker compose run --rm health-checker python health_check.py --once   # one manual pass
docker compose stop health-checker                                 # pause it
```

Each cycle it probes every distinct origin node with a signed request (manifest + first
segment). A node is marked unhealthy after `HC_FAIL_THRESHOLD` consecutive failures and
healthy again after `HC_RECOVER_THRESHOLD` successes. If *all* nodes fail at once it assumes the
problem is the checker's network and leaves flags alone. Current state per node:
`GET /api/admin/origins/health` (admin token required).

## 3. First content (bootstrap)

Real tokens come from your identity provider. To get started, mint a short-lived admin token
with the same `JWT_KEY` (add `aud`/`iss` claims if you set `JWT_AUDIENCE`/`JWT_ISSUER`):

```bash
set -a; . ./.env; set +a
export TOKEN=$(docker compose run --rm -T -e JWT_KEY api python -c "import jwt,time,os; print(jwt.encode({'sub':'admin-1','roles':['admin'],'exp':int(time.time())+3600}, os.environ['JWT_KEY'], algorithm='HS256'))")
```

Then, using `H="Authorization: Bearer $TOKEN"` against `http://127.0.0.1:8000/api/admin`:

```bash
# 1. Course (draft) with one lesson
curl -X POST $API/courses -H "$H" -H 'Content-Type: application/json' -d '{
  "slug":"intro-to-python","title":"Intro to Python","tags":["python"],
  "lessons":[{"lesson_id":"l1","title":"Welcome","order":0}]}'

# 2. Register the lesson's video across your three origin nodes (hosts must match ALLOWED_ORIGIN_HOSTS)
curl -X PUT $API/assets/vid-001 -H "$H" -H 'Content-Type: application/json' -d '{
  "course_id":"<course id from step 1>","lesson_id":"l1","title":"Welcome",
  "origins":[
   {"name":"origin-1","base_url":"https://o1.cdn.example.com/vod/vid-001/","priority":1},
   {"name":"origin-2","base_url":"https://o2.cdn.example.com/vod/vid-001/","priority":2},
   {"name":"origin-3","base_url":"https://o3.cdn.example.com/vod/vid-001/","priority":3}]}'

# 3. Publish, create a learner, enroll them
curl -X POST $API/courses/<course id>/publish -H "$H"
curl -X POST $API/users -H "$H" -H 'Content-Type: application/json' -d '{"email":"student@example.com"}'
curl -X POST $API/enrollments -H "$H" -H 'Content-Type: application/json' -d '{"user_id":"<user id>","course_id":"<course id>"}'
```

## 4. Before going live

- **CDN edge verification:** your origins must verify the `exp` and `sig` query parameters
  (HMAC-SHA256 over `<path prefix>|<exp>` with `ASSET_SIGNING_KEY`; see `verify_signature` in
  `main.py`) and send CORS headers allowing your site's origin.
- **Frontend:** set `NEXT_PUBLIC_API_BASE` to the API's public URL and add the site's origin to
  `CORS_ORIGINS`.
- **Backups:** the only state is the `mongo_data` volume, e.g.
  `docker compose exec mongo mongodump --archive --gzip -u "$MONGO_ROOT_USER" -p "$MONGO_ROOT_PASSWORD" --authenticationDatabase admin > backup.gz`.
  Redis is a disposable cache.
- **Dry run:** on staging, take one origin node down and confirm the checker flips its flag and
  the player fails over to the next node.

## Common operations

```bash
docker compose logs -f api                 # API logs
docker compose up -d --build               # deploy a new version
docker compose down                        # stop (data volume is kept)
docker compose exec api python -c "print('ok')"
```
