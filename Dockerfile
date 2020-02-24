FROM python:3.7
COPY main.py /
COPY requirements.txt /
RUN pip install -r requirements.txt
CMD ["python", "./main.py"]