FROM waggle/plugin-base:1.1.1-base

COPY requirements.txt /app/
RUN pip3 install --no-cache-dir --upgrade -r /app/requirements.txt
COPY app.py /app/

WORKDIR /app
ENTRYPOINT ["--device", "/dev/tty.usbserial-1420", "python3", "/app/app.py"]
