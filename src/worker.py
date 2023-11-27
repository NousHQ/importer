# worker.py
from rq import Worker, Queue
import redis

if __name__ == '__main__':
    redis_host = '127.0.0.1'
    redis_port = 6379  # Default Redis port

    # If you've set a password for your Redis instance, include it
    redis_password = None  # Leave as None if no password

    redis_conn = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
    q = Queue('default', connection=redis_conn)
    worker = Worker(q, connection=redis_conn)
    worker.work()
    # with Connection(redis_conn):  
    #     worker = Worker(map(Queue, ['default']))
    #     worker.work()
