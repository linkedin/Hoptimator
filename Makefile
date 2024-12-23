
install:
	./gradlew compileJava installDist

test:
	./gradlew test -x spotbugsMain -x spotbugsTest -x spotbugsTestFixtures

build:
	./gradlew build
	docker build . -t hoptimator
	docker build hoptimator-flink-runner -t hoptimator-flink-runner

bounce: build undeploy deploy deploy-samples deploy-config deploy-demo

# Integration tests expect K8s and Kafka to be running
integration-tests: deploy-dev-environment deploy-samples
	kubectl wait kafka.kafka.strimzi.io/one --for=condition=Ready --timeout=10m -n kafka
	kubectl wait kafkatopic.kafka.strimzi.io/existing-topic-1 --for=condition=Ready --timeout=10m -n kafka
	kubectl wait kafkatopic.kafka.strimzi.io/existing-topic-2 --for=condition=Ready --timeout=10m -n kafka
	kubectl port-forward -n kafka svc/one-kafka-external-0 9092 & echo $$! > port-forward.pid
	./gradlew intTest || kill `cat port-forward.pid`
	kill `cat port-forward.pid`

clean:
	./gradlew clean

deploy-config:
	kubectl create configmap hoptimator-configmap --from-file=model.yaml=test-model.yaml --dry-run=client -o yaml | kubectl apply -f -

deploy: deploy-config
	kubectl apply -f ./hoptimator-k8s/src/main/resources/
	kubectl apply -f ./deploy

quickstart: build deploy

deploy-demo: deploy
	kubectl apply -f ./deploy/samples/demodb.yaml

deploy-samples: deploy
	kubectl wait --for=condition=Established=True	\
	  crds/subscriptions.hoptimator.linkedin.com \
	  crds/kafkatopics.hoptimator.linkedin.com \
	  crds/sqljobs.hoptimator.linkedin.com
	kubectl apply -f ./deploy/samples

deploy-dev-environment: deploy-config
	kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml || echo "skipping"
	kubectl create namespace kafka || echo "skipping"
	helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.9.0/
	helm upgrade --install --atomic --set webhook.create=false flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
	kubectl apply -f "https://strimzi.io/install/latest?namespace=kafka" -n kafka
	kubectl wait --for=condition=Established=True crds/kafkas.kafka.strimzi.io
	kubectl apply -f ./hoptimator-k8s/src/main/resources/
	kubectl apply -f ./deploy/dev
	kubectl apply -f ./deploy/samples/demodb.yaml
	kubectl apply -f ./deploy/samples/kafkadb.yaml

undeploy-dev-environment:
	kubectl delete kafkatopic.kafka.strimzi.io -n kafka --all || echo "skipping"
	kubectl delete strimzi -n kafka --all || echo "skipping"
	kubectl delete pvc -l strimzi.io/name=one-kafka -n kafka || echo "skipping"
	kubectl delete -f "https://strimzi.io/install/latest?namespace=kafka" -n kafka || echo "skipping"
	kubectl delete -f ./deploy/samples/kafkadb.yaml || echo "skipping"
	kubectl delete -f ./deploy/samples/demodb.yaml || echo "skipping"
	kubectl delete -f ./deploy/dev || echo "skipping"
	kubectl delete -f ./hoptimator-k8s/src/main/resources/ || echo "skipping"
	kubectl delete namespace kafka || echo "skipping"
	helm uninstall flink-kubernetes-operator || echo "skipping"
	kubectl delete -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml || echo "skipping"

undeploy: undeploy-dev-environment
	kubectl delete -f ./deploy || echo "skipping"
	kubectl delete configmap hoptimator-configmap || echo "skipping"

generate-models:
	./generate-models.sh
	./hoptimator-models/generate-models.sh # <-- marked for deletion

release:
	test -n "$(VERSION)"  # MISSING ARG: $$VERSION
	./gradlew publish

.PHONY: build test install clean quickstart deploy-dev-environment deploy deploy-samples deploy-demo deploy-config integration-tests bounce generate-models release
