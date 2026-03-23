web: gunicorn --bind 0.0.0.0:${PORT:-5000} --workers 1 --threads 1 --timeout 120 --max-requests 500 --max-requests-jitter 50 app:app
worker: python3 -u main.py
