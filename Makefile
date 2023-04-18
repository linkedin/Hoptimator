


build:
	./gradlew build
	docker build . -t hoptimator
	docker build hoptimator-flink-runner -t hoptimator-flink-runner

clean:
	./gradlew clean

quickstart: build deploy-dev-environment deploy

deploy-dev-environment: 
	kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml || echo "skipping"
	helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.4.0/
	helm upgrade --atomic --set webhook.create=false flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator || helm install --atomic --set webhook.create=false flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
	kubectl create namespace kafka || echo "skipping"
	kubectl apply -f "https://strimzi.io/install/latest?namespace=kafka" -n kafka
	kubectl apply -f "https://strimzi.io/examples/latest/kafka/kafka-ephemeral-single.yaml" -n kafka
	kubectl apply -f ./deploy/dev

deploy:
	kubectl apply -f ./deploy/

.PHONY: build clean quickstart deploy-dev-environment deploy
