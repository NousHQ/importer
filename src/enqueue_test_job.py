# enqueue_test_job.py
from redis import Redis
from rq import Queue
from tasks import hello_world
# Setup the connection to Redis
# Replace 'your.redis.server.ip' with the IP of your Redis server
redis_conn = Redis(host='127.0.0.1', port=6379, db=0)

# Specify the queue name, default is the commonly used one
q = Queue('default', connection=redis_conn)

# Enqueue the job
job = q.enqueue('main.importer')
# job = q.enqueue(hello_world)

print(f"Job {job.id} enqueued, the job is now in the {job.origin} queue")
