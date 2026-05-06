FROM alpine

ENV WORKERS=10

RUN apk add --no-cache python3 py3-pip

RUN mkdir /app

COPY . /app

RUN python -m venv .venv

ENV PATH="/app/.venv/bin:$PATH"

RUN . .venv/bin/activate

ENV PYTHONPATH=/app

RUN .venv/bin/pip install --no-cache-dir --upgrade -r app/requirements.txt

CMD [".venv/bin/gunicorn", "-w 10",  "--bind", "0.0.0.0:80", "-t 300", "main:app"]
