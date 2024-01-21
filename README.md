docker-compose -f zk-single-kafka-single.yml up -d

docker-compose -f zk-single-kafka-single.yml ps

docker exec -it kafka1 /bin/bash

kafka-topics --version

python producer.py

python consumer.py