# Quick readme, not detailed


1. Create a cluster on GCP with node autoscaling

```
gcloud container clusters create quakeflow-cluster --zone="us-west1-a" --scopes="cloud-platform" --image-type="ubuntu"  --machine-type="n1-standard-2" --num-nodes=2 --enable-autoscaling --min-nodes 1 --max-nodes 4
```

2. Switch to the correct context

```
gcloud container clusters get-credentials quakeflow-cluster
```

3. Deploy the services on the cluster

```
kubectl apply -f quakeflow-gcp.yaml 
```

4. Setup the APIs

4.1 Add pods autoscaling
```
kubectl autoscale deployment phasenet-api --cpu-percent=80 --min=1 --max=10
kubectl autoscale deployment gmma-api --cpu-percent=80 --min=1 --max=10
kubectl autoscale deployment quakeflow-streamlit --cpu-percent=80 --min=1 --max=10
```

4.2 Expose API
```
kubectl expose deployment phasenet-api --type=LoadBalancer --name=phasenet-service
kubectl expose deployment gmma-api --type=LoadBalancer --name=phasenet-service
kubectl expose deployment quakeflow-streamlit --type=LoadBalancer --name=phasenet-service
```

5. Install Kafka

5.1 Install
```
helm install my-kafka bitnami/kafka   
```

5.2 Create topics
```
kubectl run --quiet=true -it --rm my-kafka-client --restart='Never' --image docker.io/bitnami/kafka:2.7.0-debian-10-r68 --restart=Never --command -- bash -c "kafka-topics.sh --create --topic phasenet_picks --bootstrap-server my-kafka.default.svc.cluster.local:9092 && kafka-topics.sh --create --topic gmma_events --bootstrap-server my-kafka.default.svc.cluster.local:9092 && kafka-topics.sh --create --topic waveform_raw --bootstrap-server my-kafka.default.svc.cluster.local:9092"
```

5.3 Check status
```
helm status my-kafka
```

6. Rollup restart deployments
```
kubectl rollout restart deployments   
```