# References

https://developer.confluent.io/quickstart/kafka-docker/

kafka-topics --list --bootstrap-server broker:9092

docker exec broker \                    
kafka-topics --bootstrap-server broker:9092 \
--create \
--topic quickstart

kafka-topics --bootstrap-server broker:9092 --create --topic evstx


while(true); do kafka-console-producer -bootstrap-server broker:9092 --topic quickstart < kk; done
cat 