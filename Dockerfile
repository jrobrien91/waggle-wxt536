FROM waggle/plugin-base:1.1.1-base

COPY requirements.txt /app/
RUN pip3 install --no-cache-dir --upgrade -r /app/requirements.txt
COPY app.py /app/

WORKDIR /app
ENTRYPOINT ["python3", "/app/app.py", "--device", "/dev/ttyUSB0", "--debug", "--beehive-publish-interval", "0.0"]
