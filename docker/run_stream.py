import os
import runpy
import uuid
from pathlib import Path
from kafka.admin import KafkaAdminClient, NewTopic

root = Path('/app')
run_id = uuid.uuid4().hex
state = root / 'runs' / run_id
state.mkdir(parents=True)
os.environ['STREAM_STATE_DIR'] = str(state)
os.environ['KAFKA_TOPIC'] = 'nyc311-' + run_id
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 pyspark-shell'
admin = KafkaAdminClient(bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'])
admin.create_topics([NewTopic(os.environ['KAFKA_TOPIC'], 1, 1)])
admin.close()
print(f'Run state: {state}', flush=True)
for script in ('prepare_replay.py', 'producer.py', 'consumer.py'):
    print(f'Running {script}', flush=True)
    runpy.run_path(str(root / 'src' / 'stream' / script), run_name='__main__')
