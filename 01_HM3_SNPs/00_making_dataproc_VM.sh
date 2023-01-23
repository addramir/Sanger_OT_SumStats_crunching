export CLUSTER_NAME=yt-dataporc-test
export PROJECT=open-targets-genetics-dev
export INSTANCE_ZONE=europe-west1-d
export REGION=europe-west1

gcloud dataproc clusters create ${CLUSTER_NAME} \
		--image-version=2.0 \
		--project=${PROJECT} \
		--region=${REGION} \
		--zone=${INSTANCE_ZONE} \
		--master-machine-type=n1-standard-16 \
		--enable-component-gateway \
		--single-node

		