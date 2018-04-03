FROM kernsuite/base:3
RUN docker-apt-install lofar prefactor python-pip git
ENV PYTHONPATH /usr/lib/prefactor/scripts/
RUN git clone -b prefactor-dev https://github.com/recap/pumpkin.git /root/pumpkin
RUN mkdir -p /root/.local/lib/python2.7/site-packages/
RUN cd /root/pumpkin && python setup.py install --prefix=/root/.local

