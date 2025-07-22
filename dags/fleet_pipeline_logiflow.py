import os
from datetime import datetime
from pathlib import Path

import great_expectations as gx
import numpy as np
import pandas as pd
# DAG and task decorators for interfacing with the TaskFlow API
from airflow.decorators import (
    dag,
    task,
)
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from scipy.stats import linregress


@dag(
    schedule_interval="@daily", 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        # Last-mile delivery requires immediate reliability
        "retries": 1,  # Fast failure for real-time operations
    },
    tags=["fleet_management", "last_mile", "delivery_optimization"],
)
def fleet_pipeline_logiflow():
    """### LogiFlow Last-Mile Delivery Pipeline
    
    High-precision pipeline for LogiFlow - last-mile delivery specialists.
    Optimized for short-distance, time-critical deliveries with the most
    stringent accuracy requirements in the fleet management ecosystem.
    """
    
    fleet_operator = "logiflow"
    
    start_task = DummyOperator(task_id="start")

    # Rigorous quality validation for customer-facing delivery promises
    data_quality_task = GreatExpectationsOperator(
        task_id="validate_delivery_data",
        data_context_root_dir="./dags/gx",
        
        # LogiFlow handles last-mile delivery optimization
        data_asset_name="fleet_trips_logiflow",
        dataframe_to_validate=pd.read_parquet(
            f"s3://{Variable.get('fleet_data_bucket')}/fleet_operations/datasets/"
            f"{fleet_operator}/trip_data.parquet"
        ),
        
        execution_engine="PandasExecutionEngine",
        expectation_suite_name="lastmile-quality-standards",
        return_json_dict=True,
        fail_task_on_validation_failure=True,
    )
    
    @task
    def train_delivery_model(bucket_name: str, fleet_operator: str):
        """Train high-precision model for last-mile delivery time estimation."""
        
        datasets_path = f"s3://{bucket_name}/fleet_operations/datasets"
        
        # Load last-mile delivery training data
        train_data = pd.read_parquet(f"{datasets_path}/{fleet_operator}/train.parquet")
        test_data = pd.read_parquet(f"{datasets_path}/{fleet_operator}/test.parquet")
        
        # Last-mile features: short distances, urban navigation complexity
        X_train = train_data[["delivery_distance_km"]].to_numpy()[:, 0]
        X_test = test_data[["delivery_distance_km"]].to_numpy()[:, 0]

        # Delivery time in minutes - critical for customer satisfaction
        y_train = train_data[["delivery_duration_minutes"]].to_numpy()[:, 0]
        y_test = test_data[["delivery_duration_minutes"]].to_numpy()[:, 0]

        # Train precision model for customer delivery windows
        model = linregress(X_train, y_train)

        # Evaluate on last-mile test scenarios
        y_pred_test = model.slope * X_test + model.intercept
        rmse_performance = np.sqrt(np.average((y_pred_test - y_test) ** 2))
        
        print(f"=== LogiFlow Last-Mile Performance ===")
        print(f"RMSE: {rmse_performance:.2f} minutes")
        print(f"Delivery speed factor: {model.slope:.3f} min/km")
        print(f"Base delivery overhead: {model.intercept:.2f} minutes")
        
        return rmse_performance
        
    def _evaluate_delivery_deployment(ti):
        """Strict deployment criteria for customer-facing delivery promises."""
        
        model_rmse = ti.xcom_pull(task_ids="train_delivery_model")
        
        # LogiFlow SLA: predictions within 2 minutes for customer satisfaction
        sla_threshold_minutes = 2.0  # Tightest SLA for last-mile accuracy
        
        if model_rmse < sla_threshold_minutes:
            print(f"✅ Delivery model APPROVED - RMSE: {model_rmse:.2f} min")
            print("LogiFlow customer SLA achieved for delivery windows")
            return "deploy_delivery_model"
        else:
            print(f"❌ Customer SLA not met - RMSE: {model_rmse:.2f} min")
            print("Delivery accuracy insufficient for customer promises")
            return "notify_delivery_team"

    deployment_decision = BranchPythonOperator(
        task_id="delivery_deployment_decision",
        python_callable=_evaluate_delivery_deployment,
        do_xcom_push=False,
    )

    @task
    def deploy_delivery_model():
        """Deploy model to LogiFlow customer-facing delivery system."""
        print("📦 DEPLOYING LAST-MILE MODEL")
        print("LogiFlow customer delivery prediction system updated")
        print("Integration: Real-time delivery window estimation")
        print("Customer impact: More accurate delivery promises")
        print("Expected customer satisfaction increase: 25%")
        print("Notification sent to: delivery@logiflow.com")

    @task  
    def notify_delivery_team():
        """Alert delivery operations about model accuracy concerns."""
        print("🚨 DELIVERY TEAM ALERT")
        print("Last-mile model accuracy below customer SLA")
        print("Critical actions required:")
        print("- Review delivery route complexity factors")
        print("- Analyze traffic pattern variations")
        print("- Consider real-time traffic integration")
        print("- Evaluate driver performance variations")
        print("Urgent alert sent to: delivery@logiflow.com")

    end_task = DummyOperator(
        task_id="pipeline_complete", 
        trigger_rule="none_failed_or_skipped"
    )
    
    # Streamlined pipeline for time-critical last-mile operations
    (
        start_task
        >> data_quality_task
        >> train_delivery_model(
            bucket_name="{{ var.value.fleet_data_bucket }}",
            fleet_operator=fleet_operator,
        )
        >> deployment_decision
        >> [deploy_delivery_model(), notify_delivery_team()]
        >> end_task
    )
    
# Register the DAG for LogiFlow last-mile operations
dag_fleet_pipeline_logiflow = fleet_pipeline_logiflow() 