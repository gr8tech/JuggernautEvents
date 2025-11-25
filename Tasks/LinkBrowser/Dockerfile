FROM mcr.microsoft.com/playwright/python:v1.48.0-jammy

# Install Xvfb for non-headless
RUN apt-get update && apt-get install -y \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY app.py /app/
COPY requirements.txt /app/

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install PyVirtualDisplay   
RUN pip install -r requirements.txt

# Install browsers
RUN playwright install

ENV DISPLAY=":99"

CMD ["bash", "-c", "Xvfb :99 -screen 0 1920x1080x24 & sleep 1 & python app.py"]
