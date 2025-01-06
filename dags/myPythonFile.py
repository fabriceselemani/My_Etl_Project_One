from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook                  # to install   
from airflow.decorators import task
from datetime import datetime 
from airflow.providers.postgres.hooks.postgres import PostgresHook  


# api connection
conn_id = "api_conn"

# final destination (postgres database) connection
postGres_id = "postgres_conn"


default_args = {
    'owner': 'fabrice_selemani',
    'start_date': datetime(2024, 12, 16)  # Correct usage of datetime
}


# dag
with DAG (
    dag_id='data_from_random_user',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False) as dag:



    @task(task_id="Extract_data") 
    def extract_data():
        # Use HttpHook to retrieve the URL from Airflow connection settings
        http_hook = HttpHook(method='GET', http_conn_id=conn_id)
        # Execute the GET request
        response = http_hook.run('')  # Empty string for default endpoint from connection
        if response.status_code == 200:
            return response.json()
        else:
            print("No data extracted, there's something wrong!")


    @task(task_id="process_data")
    def process_data(data_to_process):
        result = data_to_process["results"] [0]
        data = {
                    "gender" : result["gender"],
                    "first_name" : result["name"]["first"],
                    "last_name" : result["name"]["last"],
                    "address" : f"{result["location"]["street"]["number"]} {result["location"]["street"]["name"]}",
                    "city" : result["location"]["city"],
                    "state" : result["location"]["state"],
                    "country" : result["location"]["country"],
                    "postcode" : str(result["location"]["postcode"]),    # this was an integer data type (now turned to a sting)
                    "email" : result["email"],
                    "user_name" : result["login"]["username"],
                    "user_password" : result["login"]["password"],
                    "age" : result["dob"]["age"],             # this is an integer data type
                    "date_registered" : result["registered"]["date"],
                    "phone_number" : result["phone"]
                
                }
        
        return data
    

    @task(task_id="load_data")
    def load_data(data_to_load):
        postgres_hook = PostgresHook(postgres_conn_id=postGres_id)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # create the table users if it does not exist
        cursor.execute(""" CREATE TABLE IF NOT EXISTS user_infos (
                       user_id SERIAL PRIMARY KEY,
                       gender VARCHAR (50),
                       first_name VARCHAR(50),
                       last_name VARCHAR(50),
                       address VARCHAR(250),
                       city VARCHAR(250),
                       state VARCHAR(250),
                       country VARCHAR(250),
                       postcode VARCHAR(50),
                       email VARCHAR(250),
                       user_name VARCHAR(250),
                       user_password VARCHAR(250),
                       age INT,
                       date_registered VARCHAR(250),
                       phone_number VARCHAR(250)) """)
        
        
        # inserting data into the table 
        cursor.execute(
        "INSERT INTO user_infos (gender, first_name, last_name, address, city, state, country, postcode, email, user_name, user_password, age, date_registered, phone_number) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                       (data_to_load ['gender'], data_to_load['first_name'], data_to_load['last_name'], data_to_load['address'],
                        data_to_load['city'], data_to_load['state'], data_to_load['country'], data_to_load['postcode'],
                        data_to_load['email'], data_to_load['user_name'], data_to_load['user_password'], data_to_load['age'],
                        data_to_load['date_registered'], data_to_load['phone_number'])
                       )
        
        conn.commit()
        conn.close()


    data_extracted = extract_data()
    processed_data = process_data(data_extracted)
    load_data(processed_data)
