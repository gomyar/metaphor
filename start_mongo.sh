
docker compose up -d mongo
docker compose exec mongo mongosh --eval "rs.initiate()"
