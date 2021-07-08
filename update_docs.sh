pdoc --html --output-dir docs smax.smax_redis_client
mv ./docs/smax/smax_redis_client.html ./docs/index.html
rm -rf ./docs/smax/
echo "Generated new index.html"
