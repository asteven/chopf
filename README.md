# chopf

A kubernetes controller runtime for python.


## Example

The codebase includes an [example controller](./src/chaos/examples/chaospod/) which is modeled after the
[ChaosPod example in the golang controller-runtime](https://github.com/kubernetes-sigs/controller-runtime/tree/main/examples/crd).

Generate the ChaosPod custom resource definition and it to kuberntes:

```
uv run chopf crd --modules chopf.examples.chaospod > chaospod-crd.yaml
kubectl apply -f chaospod-crd.yaml
```

Run the controller:

```
uv run chopf run --modules chopf.examples.chaospod
```

Create a ChaosPod resource:

```
(
cat << DONE
apiVersion: examples.chopf/v1alpha1
kind: ChaosPod
metadata:
  name: chaos-pod
  namespace: default
spec:
  template:
    metadata:
      name: not-used
    spec:
      containers:
      - image: gcr.io/google-samples/hello-app:1.0
        name: hello-app
        ports:
        - containerPort: 8080
DONE
) | kubectl apply -f -
```

Check the ChaosPod resource:

```
% kubectl get chaospod
NAME        LAST RUN               NEXT STOP
chaos-pod   2025-06-28T20:46:15Z   2025-06-28T20:46:20Z
%
```

Watch the chaos-pod Pod being started and stopped:

```
% kubectl get --watch pod chaos-pod
NAME        READY   STATUS    RESTARTS   AGE
chaos-pod   1/1     Running   0          7s
chaos-pod   1/1     Terminating   0          21s
chaos-pod   0/1     Terminating   0          23s
chaos-pod   0/1     Terminating   0          23s
chaos-pod   0/1     Terminating   0          23s
chaos-pod   0/1     Pending       0          0s
chaos-pod   0/1     Pending       0          0s
chaos-pod   0/1     ContainerCreating   0          0s
chaos-pod   0/1     ContainerCreating   0          3s
chaos-pod   1/1     Running             0          6s
chaos-pod   1/1     Terminating         0          21s
...
```
