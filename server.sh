
export SECRET_KEY=keepitsecretkeepitsafe
export SERVER_NAME=localhost:8000
export FLASK_APP=server.py
export FLASK_DEBUG=1
export METAPHOR_DBNAME=metaphor
gunicorn server:app --timeout 6000
