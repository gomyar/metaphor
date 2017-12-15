
export FLASK_APP=server.py
export FLASK_DEBUG=1
gunicorn server:app
