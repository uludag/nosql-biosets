FROM python:3
ENV PYTHONUNBUFFERED 1

LABEL \
  description="Index/query scripts for selected free bioinformatics datasets" \
  maintainer="mahmut.uludag@kaust.edu.sa"

RUN \
  pip install pytz pivottablejs

RUN \
  pip install git+https://github.com/uludag/nosql-biosets.git
