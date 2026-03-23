FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    ffmpeg \
    aria2 \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN curl -L "https://www.bok.net/Bento4/binaries/Bento4-SDK-1-6-0-641.x86_64-unknown-linux.zip" \
        -o /tmp/bento4.zip && \
    python3 -c "\
import zipfile, os; \
z = zipfile.ZipFile('/tmp/bento4.zip'); \
matches = [n for n in z.namelist() if 'mp4decrypt' in n and not n.endswith('/')]; \
data = z.read(matches[0]); \
open('/app/mp4decrypt', 'wb').write(data); \
os.chmod('/app/mp4decrypt', 0o755)" && \
    rm /tmp/bento4.zip

COPY *.py .
COPY database.json .

RUN mkdir -p downloads

ENV PYTHONUNBUFFERED=1
ENV PYTHONOPTIMIZE=1
ENV PYTHONASYNCIODEBUG=0
ENV PYTHONDONTWRITEBYTECODE=1

CMD ["sh", "-c", "rm -rf /app/downloads/* && python3 -u main.py"]
