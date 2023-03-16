FROM ubuntu:20.04

SHELL ["/bin/bash", "-c"]

USER root

ENV DEBIAN_FRONTEND noninteractive
ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
ENV PATH="${PATH:+$PATH:}/usr/local/bin:/home/default/.local/bin"
ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH:+$LD_LIBRARY_PATH:}/usr/local/lib"
ENV PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}:/usr/local/lib/python3.8/site-packages"

RUN apt-get update &&                                                        \
    apt-get install -y dialog &&                                             \
    apt-get install -y apt-utils &&                                          \
    apt-get upgrade -y &&                                                    \
    apt-get install -y sudo

# This adds the 'default' user to sudoers with full privileges:
RUN HOME=/home/default &&                                                    \
    mkdir -p ${HOME} &&                                                      \
    GROUP_ID=1000 &&                                                         \
    USER_ID=1000 &&                                                          \
    groupadd -r default -f -g "$GROUP_ID" &&                                 \
    useradd -u "$USER_ID" -r -g default -d "$HOME" -s /sbin/nologin          \
    -c "Default Application User" default &&                                 \
    chown -R "$USER_ID:$GROUP_ID" ${HOME} &&                                 \
    usermod -a -G sudo default &&                                            \
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN apt-get install -y     \
        git                \
        wget               \
        curl               \
        zip                \
        python3            \
        python3-venv       \
        python3-pip        \
        python3-dev        \
        python3-dev        \
        libxft-dev         \
        libfreetype6       \
        libfreetype6-dev

RUN rm -f /usr/bin/python && ln -s /usr/bin/python3 /usr/bin/python && \
    rm -f /usr/bin/pip && ln -s /usr/bin/pip3 /usr/bin/pip

RUN pip3 install pipenv

USER default
WORKDIR /home/default

RUN curl -s "https://get.sdkman.io" | bash
RUN source ~/.sdkman/bin/sdkman-init.sh &&\
    sdk install java 15.0.2-open &&\
    sdk use java 15.0.2-open &&\
    sdk install maven 3.8.4 &&\
    sdk use maven 3.8.4

RUN mkdir workdir
WORKDIR workdir
