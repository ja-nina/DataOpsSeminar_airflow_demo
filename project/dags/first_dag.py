try:
    from transformers import T5Tokenizer, T5ForConditionalGeneration
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def first_function_execute(**context):
    print("first_function_execute   ")
    context['ti'].xcom_push(key='mykey', value="first says Hello, it's nice to have you here! I love data :) ")


def second_function_execute(**context):
    instance = context.get("ti").xcom_pull(key="mykey")
    data = [{"name":"Nina","title":"DataOpsussy person"}, { "name":"Oska","title":"ANOTHER DataOpsussy person"}, { "name":"Oska","title":"Also DataOpsussy person"},]
    df = pd.DataFrame(data=data)
    print('@'*66)
    print("The data specified:")
    print(df.head())
    print('@'*66)
    print("I am in second_function_execute got value :{} from Function 1  ".format(instance))


def transformer_function_execute(**context):
    print("transformer_function_execute   ")
    first_function_sentence = context.get("ti").xcom_pull(key="mykey")
    tokenizer = T5Tokenizer.from_pretrained("t5-small")
    model = T5ForConditionalGeneration.from_pretrained("t5-small")
    input_ids = tokenizer("translate English to German: {}".format(first_function_sentence), return_tensors="pt").input_ids
    outputs = model.generate(input_ids)
    print(" transformed sentence: ", tokenizer.decode(outputs[0], skip_special_tokens=True))


with DAG(
        dag_id="first_dag", #best practise is for it to be like like the file
        schedule_interval="@daily", # can be chron
        default_args={
            "owner": "airflow",
            "retries": 1, #if fails
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2023, 3, 29), # catchup is re-doing old
        },
        catchup=False) as f:

    first_function_execute = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute,
        provide_context=True, # crucial for data exchange
        op_kwargs={"name":"DataOpsussy"}
    )

    second_function_execute = PythonOperator(
        task_id="second_function_execute",
        python_callable=second_function_execute,
        provide_context=True,
    )

    transformer_function_execute = PythonOperator(
        task_id="transformer_function_execute",
        python_callable=transformer_function_execute,
        provide_context=True,
    )

first_function_execute >> second_function_execute >> transformer_function_execute


