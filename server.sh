
export SECRET_KEY=keepitsecretkeepitsafe
export SERVER_NAME=localhost:8000
export FLASK_APP=server:app
export FLASK_DEBUG=1
export METAPHOR_DBNAME=metaphor
export METAPHOR_MONGO_HOST=localhost

export FLASK_RUN_HOST=127.0.0.1
export FLASK_RUN_PORT=8000

#gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker server:app --timeout 6000
python src/server.py
