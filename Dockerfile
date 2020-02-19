FROM python:3.7
COPY scratch.py /
COPY requirements.txt /
RUN pip install -r requirements.txt
CMD ["python", "./scratch.py"]