FROM totvslabs/pycarol:2.34.3

RUN mkdir /app
WORKDIR /app
ADD requirements.txt /app/
RUN pip install -r requirements.txt

ADD . /app

CMD ["python", "run.py"]
