FROM fserena/python-base
LABEL maintainer=kudhmud@gmail.com

RUN apt-get update && apt-get install -y redis-server redis-tools
RUN mkdir /data
RUN /root/.env/bin/pip install git+https://github.com/fserena/wd-entities.git
