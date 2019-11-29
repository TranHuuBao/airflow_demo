# Tại sao phải là Airflow:
- Bạn có cần schedule các tác vụ của mình chạy trên server?
- Bạn sử dụng crontab?
- Bạn làm sao để xử lý khi nó bị lỗi
  - Bạn outlog ra file rồi check file log
  - Bạn sử dụng healthchecks.io để alert
  - Vậy bạn làm sao để chạy lại các tác vụ đã lỗi nhanh chóng nhất
 - Bạn có thường chạy nhiều job  liên tục với nhau không?
   - Bạn làm sao để chạy được như vậy?
  - Vậy khi bạn muốn chạy song song các job đồng thời thì bạn xử lý như thế nào?
  
  Airflow sinh ra để xử lý tất cả các vấn đề trên. Bạn có thể chạy liên tục các job, đồng thời các job, cảnh báo nếu có lỗi hoặc cảnh báo thành công và nhiều hơn như thế nữa. 
# Một số  Operator cơ bản trong Airflow

## Python Operator 
```
import airflow

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
import time
import pendulum
local_tz = pendulum.timezone('Asia/Saigon')
dag = DAG(
    dag_id='python_operator',
    schedule_interval='@once',
    dagrun_timeout=timedelta(minutes=30),
    start_date=datetime(2019, 10, 28, 7, 5, tzinfo=local_tz),
    catchup=False
)
def print_context(ds, **kwargs):
    print(ds)
    return 'Whatever you return gets printed in the logs'


run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)
run_this
```

## Bash Operator 
```
import airflow

from airflow.models import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import timedelta, datetime
from utils.operators.slack import task_fail_slack_alert
from utils.operators.slack import task_success_slack_alert
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

import pendulum

args = {
    'owner': 'root',
    'run_as_user': 'root',
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 0,
    'on_failure_callback': task_fail_slack_alert,
    'depends_on_past': False
}

local_tz = pendulum.timezone('Asia/Saigon')

dag = DAG(
    dag_id='bash_operator',
    schedule_interval='* 1 * * *',
    dagrun_timeout=timedelta(minutes=30),
    start_date=datetime(2019, 10, 28, 7, 5, tzinfo=local_tz),
    catchup=False,
    default_args=args
)
notify = PythonOperator(
    task_id='notify',
    python_callable=task_success_slack_alert,
    provide_context=True,
    dag=dag
)


bash_script = BashOperator(
    task_id='bash_script',
    dag=dag,
    bash_command="echo bash_operator"
)
bash_script >> notify
```

## Dummy Operator 
```
import airflow

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowSkipException
from utils.operators.slack import task_fail_slack_alert
from utils.operators.slack import task_success_slack_alert
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 0,
    'on_failure_callback': task_fail_slack_alert
}

dag = DAG(
    dag_id='dummy_operator',
    schedule_interval=None,
    default_args=default_args
)

notify = PythonOperator(
    task_id='notify',
    python_callable=task_success_slack_alert,
    provide_context=True,
    dag=dag
)

error_task = BashOperator(
    task_id='error_task',
    dag=dag,
    bash_command="java -v"
)

task_1 = BashOperator(
    task_id='test_alert_2',
    dag=dag,
    bash_command="echo one_success"
)

dummyTask = DummyOperator(task_id='task1', dag=dag, trigger_rule='one_success')

[error_task,task_1]>> dummyTask >> notify
```
## Spark Operator 
```
import airflow

from airflow.models import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import timedelta, datetime
from utils.operators.slack import task_fail_slack_alert
from utils.operators.slack import task_success_slack_alert
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

import pendulum

args = {
    'owner': 'root',
    'run_as_user': 'root',
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 0,
    'on_failure_callback': task_fail_slack_alert,
    'depends_on_past': False
}

local_tz = pendulum.timezone('Asia/Saigon')

dag = DAG(
    dag_id='spark_operator',
    schedule_interval='5 * * * *',
    dagrun_timeout=timedelta(minutes=30),
    start_date=datetime(2019, 10, 28, 7, 5, tzinfo=local_tz),
    catchup=False,
    default_args=args
)
notify = PythonOperator(
    task_id='notify',
    python_callable=task_success_slack_alert,
    provide_context=True,
    dag=dag
)

spark_job = SparkSubmitOperator(
        task_id='spark_job',
        conn_id='spark_default',
        application='/home/member1/airflow-dags/demo/jars/spark_2.11-0.0.1.jar',
        java_class='vn.fpt.spark.WordCount',
        application_args=['/user/root/input/wordcount-log.log','/user/root/output/vinhdp4/wordcount.txt'],
        executor_cores=1,
        executor_memory='1G',
        num_executors=1,
        name='spark-mapreduce-demo-airflow',
        dag=dag
        )

spark_job >> notify 
```
## Tutorial
### Exercise 1: Python Operator with parameter 

Bạn hãy tạo 1 dags PythonOperator chạy một  function in ra các từng giá trị trong list ['one', 'two', 'three','four', 'five'] bằng cách truyền vào args từ PythonOperator 
- Lưu ý: Mỗi người cần tạo một dag_id  riêng  biệt để tránh trùng lặp với format [mail_prefix]_your_dag_name
- Các dag sau khi tạo xong copy lên server 118.68.168.182 với
``` 
	account: member1 
	password: 1234567
```
vào đường dẫn /home/member1/airflow-dags/ 
Bạn có thể tạo folder riêng cho mình trong đó để dễ quản lý hơn.

### Exercise 2:  Spark Operator 
- Mỗi khi bạn submit job Spark, bạn cần kiểm tra xem input của mình đã tồn tại hay chưa hoặc output của mình đã tồn tại hay chưa. Điều đó gây mất khá nhiều thời gian và công sức.
-  Bằng Airflow và các operator đã giới thiệu , bạn hãy viết một dags có thể tối ưu hóa công việc của mình trong quá trình này. 
Với các job này sẽ submit mỗi ngày một lần vào 4h thứ 4 hàng tuần .
