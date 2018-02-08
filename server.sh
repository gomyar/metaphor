
export FLASK_APP=server.py
export FLASK_DEBUG=1
export METAPHOR_DBNAME=metaphor
gunicorn server:app --timeout 6000
