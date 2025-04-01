echo $GOOGLE_SERVICE_ACCOUNT | base64 -d > ~/.gcp-service-account.json
echo "GOOGLE_APPLICATION_CREDENTIALS=$HOME/.gcp-service-account.json" > $GITHUB_ENV

echo $DBT_APPLICATION | base64 -d > src/test/resources/application-test.yml