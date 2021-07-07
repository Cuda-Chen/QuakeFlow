![QuakeFlow logo](https://github.com/wayneweiqiang/QuakeFlow/blob/master/docs/assets/logo.png)

## A Quick Glance

QuakeFlow is a scalable deep-learning-based earthquake monitoring system with cloud computing. It applies the state-of-art deep learning/machine learning models for earthquake detection. With auto-scaling enabled on Kubernetes, our system can balance computational loads with computational resources. 

Checkout our Twitter Bot for realtime earthquake early warning at [@Quakeflow_Bot](https://twitter.com/QuakeFlow_bot).


## Deployment

We designed our system in a way that it's easy to deploy, reproducible and flexible where users can deploy it on any platform of their choice.

Basically, it's a one-liner deployment.

```
kubectl apply -f quakeflow-gcp.yaml 
```

To deploy the system on GCP, check out the [GCP Readme](gcp_readme.md).

To deploy the system on Kubernetes in general, check out the [Kubernetes Readme](k8s_readme.md).

To run the code locally, check out the guide [here](kafka-spark).

## User-Facing Platform

### Streamlit Web App

<img src="https://i.imgur.com/xL696Yh.jpg" width="800px">


### Twitter Bot

<img src="https://i.imgur.com/50kVK4Q.png" width="400px">

