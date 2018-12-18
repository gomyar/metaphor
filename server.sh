
export SECRET_KEY=keepitsecretkeepitsafe
export SERVER_NAME=localhost:8000
export FLASK_APP=server.py
export FLASK_DEBUG=1
export METAPHOR_DBNAME=metaphor
export METAPHOR_MONGO_HOST=10.131.79.24
gunicorn server:app --timeout 6000
