
build:
	./gradlew build
	docker build . -t hoptimator
	docker build hoptimator-flink-runner -t hoptimator-flink-runner

bounce: build undeploy deploy deploy-samples deploy-config

integration-tests:
	./bin/hoptimator --run=./integration-tests.sql
	echo "\nPASS"

clean:
	./gradlew clean

deploy: deploy-config
	kubectl apply -f ./deploy

undeploy:
	kubectl delete -f ./deploy || echo "skipping"
	kubectl delete configmap hoptimator-configmap || echo "skipping"

quickstart: build deploy-dev-environment deploy

deploy-dev-environment: 
	kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml || echo "skipping"
	kubectl create namespace kafka || echo "skipping"
	kubectl create namespace mysql || echo "skipping"
	helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.4.0/
	helm upgrade --install --atomic --set webhook.create=false flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
	kubectl apply -f "https://strimzi.io/install/latest?namespace=kafka" -n kafka
	kubectl wait --for=condition=Established=True crds/kafkas.kafka.strimzi.io
	kubectl apply -f ./deploy/dev

deploy-samples: deploy
	kubectl wait --for=condition=Established=True	\
	  crds/subscriptions.hoptimator.linkedin.com \
	  crds/kafkatopics.hoptimator.linkedin.com \
	  crds/sqljobs.hoptimator.linkedin.com
	kubectl apply -f ./deploy/samples

deploy-config:
	kubectl create configmap hoptimator-configmap --from-file=model.yaml=test-model.yaml --dry-run=client -o yaml | kubectl apply -f -

generate-models:
	./models/generate-models.sh

release:
	test -n "$(VERSION)"  # MISSING ARG: $$VERSION
	./gradlew publish

.PHONY: build clean quickstart deploy-dev-environment deploy deploy-samples deploy-config integration-tests bounce generate-models release
