FROM totvslabs/pycarol:2.34.3

RUN mkdir /app
WORKDIR /app
ADD requirements.txt /app/
RUN pip install -r requirements.txt

ADD . /app

RUN wget https://public.ukp.informatik.tu-darmstadt.de/reimers/sentence-transformers/v0.2/distiluse-base-multilingual-cased.zip --no-verbose -P /tmp/
RUN unzip /tmp/distiluse-base-multilingual-cased.zip -d /app/model

RUN rm -rf tmp

CMD ["python", "run.py"]
