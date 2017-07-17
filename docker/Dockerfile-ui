FROM ubuntu:16.04

EXPOSE 3000

EXPOSE 5000

RUN apt-get update && \
    apt-get install -y \
        python3 \
        python3-pip \
        wget \
        pkg-config \
        libpng-dev \
        libfreetype6-dev \
        libblas-dev \
        liblapack-dev \
        lib32ncurses5-dev \
        gfortran && \
    wget -O - https://deb.nodesource.com/setup_6.x | bash && \
    apt-get update && \
    apt-get install -y nodejs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Installs upgraded pip3 as pip3.4. pip3 continues to refer to apt's pip3.
RUN pip3 install --upgrade pip

COPY requirements.txt /tmp/

RUN pip3 install -r /tmp/requirements.txt --no-cache-dir

RUN python3 -c "import matplotlib; import matplotlib.pyplot"

ENV FLASK_APP=app.py
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

COPY . /aurum

RUN cd /aurum/UI && \
    npm install

RUN sed -i "s/^\s*db_host\s*=.*$/db_host = 'elasticsearch'/" /aurum/config.py

RUN sed -i "s|^\s*path_to_serialized_model\s*=\s*parentdir\s*\+\s*.*$|path_to_serialized_model = parentdir + '/data/pickles/'|" /aurum/frontend/app.py

CMD (cd /aurum/UI/ && npm run watch &>/dev/null &) && \
    (cd /aurum/frontend/ && flask run --host=0.0.0.0)
