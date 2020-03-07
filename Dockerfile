from pypy:3

RUN pip install --no-cache-dir \
        merkletools \
        numpy \
        pysha3 \
        pyyaml \
        sha3

WORKDIR /root/
COPY . .

CMD ./run.sh $proto $sim