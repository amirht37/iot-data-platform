
FROM python:3.11-slim

WORKDIR /app


COPY requirements.txt .


RUN pip install --no-cache-dir \
    --default-timeout=100 \
    -r requirements.txt


COPY . .


COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 5000

ENTRYPOINT ["/entrypoint.sh"]

CMD ["gunicorn", "-c", "gunicorn_config.py", "flask_app:app"]
