This project runs a kafka pipeline deployed on kubernetes via openshift.

Producer- Input is taken via the kafka shell, kafka-console-producer
Consumer-Java app run using maven
Queue DB- MongoDB also deployed on openshift at the moment 
Kafka.yaml-Used to deploy kafka on openshift using its bitnami(apache image)
Kafka-consumer-deployment.yaml-Used to deploy the docker image of the consumer app on opernshift
Mongo-deployment.yaml-Deploy mongo as well as the service on openshift
Mongo-pvc.yaml-Ensures that thew data is stored outside the pod filesystem so that data persists even if pod crashes

Run using jdk17

Refer to Demo-rec.mov for short rec of the app working