from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default args pour le DAG
default_args = {
    'owner': 'data-engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 20),
}

# Définir le DAG
dag = DAG(
    dag_id='test_dag_simple',
    default_args=default_args,
    description='DAG de test - affiche du texte',
    schedule='@daily',  
    catchup=False,
)

# Fonction Python à exécuter
def print_hello():
    print("=" * 50)
    print(" BONJOUR! Le DAG fonctionne!")
    print("=" * 50)
    return "Task exécutée avec succès."

def print_info():
    print("=" * 50)
    print("📊 Informations du DAG:")
    print(f"Timestamp: {datetime.now()}")
    print(f"Environnement: Airflow 3.1.6 + Python 3.12")
    print("=" * 50)
    return "Info affichée."

def print_end():
    print("=" * 50)
    print("🏁 FIN DU DAG - Tous les tasks sont terminés!")
    print("=" * 50)
    return "DAG terminé."

# Créer les tasks
task_1 = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='print_info',
    python_callable=print_info,
    dag=dag,
)

task_3 = PythonOperator(
    task_id='print_end',
    python_callable=print_end,
    dag=dag,
)

# Définir l'ordre des tasks
task_1 >> task_2 >> task_3