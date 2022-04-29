# kafkatp

docker run -v /home/matteo/Documents/WaterViewDC/node-ftp/credentials.json:/credentials.json -e KAFKATP_CREDENTIALS=credentials.json -e KAFKATP_KAFKAURL=redpanda -e KAFKATP_KAFKAPORT=9092 -e KAFKATP_PASVURL=127.0.0.1 -e KAFKATP_P^CVMIN=60200 -e KAFKATP_PASVMAX=60210 --network local_net -p 21:21 -p 60200-60210:60200-60210 kftp
