# networkbuildercoordinator
FROM ubuntu:18.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        python3 \
        python3-dev \
        python3-pip \
        python3-setuptools \
        lib32ncurses5-dev \
        libfreetype6-dev \
        libpng-dev \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Installs upgraded pip3 as pip3.4. pip3 continues to refer to apt's pip3.
RUN pip3 install --upgrade pip

COPY requirements.txt /tmp/

RUN pip3 install -r /tmp/requirements.txt --no-cache-dir --ignore-installed

RUN python3 -c "import matplotlib; import matplotlib.pyplot"

COPY . /aurum

# Expect an elasticsearch server available at this host and port.
# RUN echo "db_host = 'elasticsearch'\n\
# db_port = '9200'" > /aurum/local_config.py

RUN sed -i "s/^\s*db_host\s*=.*$/db_host = 'elasticsearch'/" /aurum/config.py

VOLUME /output

WORKDIR /aurum

CMD ["python3", "networkbuildercoordinator.py", "--opath", "/output"]
