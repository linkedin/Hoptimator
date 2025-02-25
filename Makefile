
install:
	./gradlew compileJava installDist

test:
	./gradlew test -x spotbugsMain -x spotbugsTest -x spotbugsTestFixtures

build:
	./gradlew build shadowJar
	docker build . -t hoptimator
	docker build hoptimator-flink-runner -f hoptimator-flink-runner/Dockerfile-flink-runner -t hoptimator-flink-runner
	docker build hoptimator-flink-runner -f hoptimator-flink-runner/Dockerfile-flink-operator -t hoptimator-flink-operator

bounce: build undeploy deploy deploy-samples deploy-config

clean:
	./gradlew clean

deploy-config:
	kubectl apply -f ./deploy/config/hoptimator-configmap.yaml

undeploy-config:
	kubectl delete configmap hoptimator-configmap || echo "skipping"

deploy: deploy-config
	kubectl apply -f ./hoptimator-k8s/src/main/resources/
	kubectl apply -f ./deploy
	kubectl apply -f ./deploy/dev/rbac.yaml

undeploy: undeploy-config
	kubectl delete -f ./deploy/dev/rbac.yaml || echo "skipping"
	kubectl delete -f ./deploy || echo "skipping"
	kubectl delete -f ./hoptimator-k8s/src/main/resources/ || echo "skipping"

quickstart: build deploy

deploy-samples: deploy
	kubectl wait --for=condition=Established=True	\
	  crds/subscriptions.hoptimator.linkedin.com \
	  crds/kafkatopics.hoptimator.linkedin.com \
	  crds/sqljobs.hoptimator.linkedin.com
	kubectl apply -f ./deploy/samples

undeploy-samples: undeploy
	kubectl delete -f ./deploy/samples || echo "skipping"

deploy-flink: deploy
	kubectl create namespace flink || echo "skipping"
	kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml || echo "skipping"
	helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.9.0/
	helm upgrade --install --atomic --set webhook.create=false,image.pullPolicy=Never,image.repository=docker.io/library/hoptimator-flink-operator,image.tag=latest --set-json='watchNamespaces=["default","flink"]' flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
	kubectl apply -f deploy/dev/flink-session-cluster.yaml
	kubectl apply -f deploy/dev/flink-sql-gateway.yaml
	kubectl apply -f deploy/samples/flink-template.yaml

undeploy-flink:
	kubectl delete flinksessionjobs.flink.apache.org --all || echo "skipping"
	kubectl delete flinkdeployments.flink.apache.org --all || echo "skipping"
	kubectl delete crd flinksessionjobs.flink.apache.org || echo "skipping"
	kubectl delete crd flinkdeployments.flink.apache.org || echo "skipping"
	helm uninstall flink-kubernetes-operator || echo "skipping"
	helm repo remove flink-operator-repo || echo "skipping"
	kubectl delete -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml || echo "skipping"

deploy-kafka: deploy deploy-flink
	kubectl create namespace kafka || echo "skipping"
	kubectl apply -f "https://strimzi.io/install/latest?namespace=kafka" -n kafka
	kubectl wait --for=condition=Established=True crds/kafkas.kafka.strimzi.io
	kubectl apply -f ./deploy/samples/kafkadb.yaml

undeploy-kafka:
	kubectl delete kafkatopic.kafka.strimzi.io -n kafka --all || echo "skipping"
	kubectl delete strimzi -n kafka --all || echo "skipping"
	kubectl delete pvc -l strimzi.io/name=one-kafka -n kafka || echo "skipping"
	kubectl delete -f "https://strimzi.io/install/latest?namespace=kafka" -n kafka || echo "skipping"
	kubectl delete -f ./deploy/samples/kafkadb.yaml || echo "skipping"
	kubectl delete -f ./deploy/samples/demodb.yaml || echo "skipping"
	kubectl delete namespace kafka || echo "skipping"

# Deploys Venice cluster in docker and creates two stores in Venice. Stores are not managed via K8s for now.
deploy-venice: deploy deploy-flink
	docker compose -f ./deploy/docker/venice/docker-compose-single-dc-setup.yaml up -d --wait
	docker exec venice-client ./create-store.sh http://venice-controller:5555 venice-cluster0 test-store schemas/keySchema.avsc schemas/valueSchema.avsc
	docker exec venice-client ./create-store.sh http://venice-controller:5555 venice-cluster0 test-store-1 schemas/keySchema.avsc schemas/valueSchema.avsc
	kubectl apply -f ./deploy/samples/venicedb.yaml

undeploy-venice:
	kubectl delete -f ./deploy/samples/venicedb.yaml || echo "skipping"
	docker compose -f ./deploy/docker/venice/docker-compose-single-dc-setup.yaml down

deploy-dev-environment: deploy deploy-flink deploy-kafka deploy-venice
	kubectl wait --for=condition=Established=True	\
	  crds/subscriptions.hoptimator.linkedin.com \
	  crds/kafkatopics.hoptimator.linkedin.com \
	  crds/sqljobs.hoptimator.linkedin.com
	kubectl apply -f ./deploy/dev/
	kubectl apply -f ./deploy/samples/demodb.yaml

undeploy-dev-environment: undeploy-venice undeploy-kafka undeploy-flink undeploy
	kubectl delete -f ./deploy/dev || echo "skipping"
	kubectl delete -f ./deploy/samples/demodb.yaml || echo "skipping"

# Integration test setup intended to be run locally
integration-tests: deploy-dev-environment
	kubectl wait kafka.kafka.strimzi.io/one --for=condition=Ready --timeout=10m -n kafka
	kubectl wait kafkatopic.kafka.strimzi.io/existing-topic-1 --for=condition=Ready --timeout=10m
	kubectl wait kafkatopic.kafka.strimzi.io/existing-topic-2 --for=condition=Ready --timeout=10m
	kubectl port-forward -n kafka svc/one-kafka-external-bootstrap 9092 & echo $$! > port-forward.pid
	kubectl port-forward -n flink svc/flink-sql-gateway 8083 & echo $$! > port-forward-2.pid
	kubectl port-forward -n flink svc/basic-session-deployment-rest 8081 & echo $$! > port-forward-3.pid
	./gradlew intTest || kill `cat port-forward.pid port-forward-2.pid, port-forward-3.pid`
	kill `cat port-forward.pid`
	kill `cat port-forward-2.pid`
	kill `cat port-forward-3.pid`

# kind cluster used in github workflow needs to have different routing set up, avoiding the need to forward kafka ports
integration-tests-kind: deploy-dev-environment
	kubectl wait kafka.kafka.strimzi.io/one --for=condition=Ready --timeout=10m -n kafka
	kubectl wait kafkatopic.kafka.strimzi.io/existing-topic-1 --for=condition=Ready --timeout=10m -n kafka
	kubectl wait kafkatopic.kafka.strimzi.io/existing-topic-2 --for=condition=Ready --timeout=10m -n kafka
	./gradlew intTest -i

generate-models:
	./generate-models.sh
	./hoptimator-models/generate-models.sh # <-- marked for deletion

release:
	test -n "$(VERSION)"  # MISSING ARG: $$VERSION
	./gradlew publish

build-zeppelin: build
	docker build -t hoptimator-zeppelin -t hoptimator-zeppelin:0.11.2 -f ./deploy/docker/zeppelin/Dockerfile-zeppelin .

# attaches to terminal (not run as daemon)
run-zeppelin: build-zeppelin
	kubectl apply -f deploy/docker/zeppelin/zeppelin-flink-engine.yaml
	kubectl apply -f deploy/docker/zeppelin/zeppelin-kafkadb.yaml
	docker run --rm -p 8080:8080 \
	  --volume=${HOME}/.kube/config:/opt/zeppelin/.kube/config \
	  --add-host=docker-for-desktop:host-gateway \
	  --name hoptimator-zeppelin \
	  hoptimator-zeppelin

.PHONY: install test build bounce clean quickstart deploy-config undeploy-config deploy undeploy deploy-samples undeploy-samples deploy-flink undeploy-flink deploy-kafka undeploy-kafka deploy-venice undeploy-venice build-zeppelin run-zeppelin integration-tests integration-tests-kind deploy-dev-environment undeploy-dev-environment generate-models release
