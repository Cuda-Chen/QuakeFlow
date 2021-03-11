# Quick readme, not detailed


## Prebuilt Kafka 

1. Install
```
helm install my-kafka bitnami/kafka     
```

2. Check the instructions to setup the client and topics
```
helm status my-kafka
```

## Our own containers

1. Switch to minikube environment
```
eval $(minikube docker-env)     
```

2. Build the docker images, see the docs for each container

```
docker build --tag quakeflow-spark:1.0 .
...
```

3. Create everything
```
kubectl apply -f quakeflow-delpoyment.yaml     
```

4. Check the pods
```
kubectl get pods
```

5. Check the logs (an example)
```
kubectl logs quakeflow-spark-7699cd45d8-mvv6r
```

6. Delete a single deployment
```
kubectl delete deploy quakeflow-spark     
```

7. Delete everything
```
kubectl delete -f quakeflow-delpoyment.yaml   
```

