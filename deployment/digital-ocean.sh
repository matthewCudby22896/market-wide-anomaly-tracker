doctl auth init --access-token $DIGITAL_OCEAN_API_KEY
set -a
source .env
set -a
TMP=$(mktemp)
echo $TMP

envsubst < deployment/setup.sh > $TMP

doctl compute droplet create replayengine-server \
	--region lon1 \
	--size s-1vcpu-1gb \
	--image docker-20-04 \
	--wait \
	--ssh-keys 56375404 \
	--user-data-file ${TMP}

# doctl compute droplet delete replayengine-server --force

