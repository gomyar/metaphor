
docker-compose up -d mongo
docker-compose exec mongo mongo --eval "rs.initiate()"
