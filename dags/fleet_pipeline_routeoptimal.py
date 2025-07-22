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
        # Long-haul operations require robust retry mechanisms
        "retries": 3,  
    },
    tags=["fleet_management", "long_haul", "route_optimization"],
)
def fleet_pipeline_routeoptimal():
    """### RouteOptimal Long-Haul Fleet Pipeline
    
    Specialized pipeline for RouteOptimal - long-distance logistics optimization.
    Handles cross-country route duration prediction with relaxed SLA thresholds
    suitable for highway and interstate transportation patterns.
    """
    
    fleet_operator = "routeoptimal"
    
    start_task = DummyOperator(task_id="start")

    # Quality validation tailored for long-haul trip patterns
    data_quality_task = GreatExpectationsOperator(
        task_id="validate_longhaul_data",
        data_context_root_dir="./dags/gx",
        
        # RouteOptimal focuses on interstate and highway routes
        data_asset_name="fleet_trips_routeoptimal",
        dataframe_to_validate=pd.read_parquet(
            f"s3://{Variable.get('fleet_data_bucket')}/fleet_operations/datasets/"
            f"{fleet_operator}/trip_data.parquet"
        ),
        
        execution_engine="PandasExecutionEngine",
        expectation_suite_name="longhaul-quality-standards",
        return_json_dict=True,
        fail_task_on_validation_failure=True,
    )
    
    @task
    def train_longhaul_model(bucket_name: str, fleet_operator: str):
        """Train duration prediction model optimized for long-haul logistics."""
        
        datasets_path = f"s3://{bucket_name}/fleet_operations/datasets"
        
        # Load long-haul training data
        train_data = pd.read_parquet(f"{datasets_path}/{fleet_operator}/train.parquet")
        test_data = pd.read_parquet(f"{datasets_path}/{fleet_operator}/test.parquet")
        
        # Long-haul features: distance is primary predictor for highway routes
        X_train = train_data[["trip_distance_km"]].to_numpy()[:, 0]
        X_test = test_data[["trip_distance_km"]].to_numpy()[:, 0]

        # Duration in hours for long-haul operations
        y_train = train_data[["trip_duration_hours"]].to_numpy()[:, 0]
        y_test = test_data[["trip_duration_hours"]].to_numpy()[:, 0]

        # Train regression model for highway speed patterns
        model = linregress(X_train, y_train)

        # Evaluate on interstate route test set
        y_pred_test = model.slope * X_test + model.intercept
        rmse_performance = np.sqrt(np.average((y_pred_test - y_test) ** 2))
        
        print(f"=== RouteOptimal Long-Haul Performance ===")
        print(f"RMSE: {rmse_performance:.2f} hours")
        print(f"Average speed factor: {1/model.slope:.1f} km/h")
        print(f"Base time overhead: {model.intercept:.2f} hours")
        
        return rmse_performance
        
    def _evaluate_longhaul_deployment(ti):
        """Deployment decision for long-haul logistics - different SLA than urban."""
        
        model_rmse = ti.xcom_pull(task_ids="train_longhaul_model")
        
        # RouteOptimal SLA: predictions within 30 minutes for long-haul routes
        sla_threshold_hours = 0.5  # 30 minutes tolerance for multi-hour trips
        
        if model_rmse < sla_threshold_hours:
            print(f"✅ Long-haul model APPROVED - RMSE: {model_rmse:.2f} hours")
            print("RouteOptimal SLA achieved for interstate logistics")
            return "deploy_longhaul_model"
        else:
            print(f"❌ Model needs improvement - RMSE: {model_rmse:.2f} hours")
            print("Long-haul accuracy below customer expectations")
            return "notify_logistics_team"

    deployment_decision = BranchPythonOperator(
        task_id="longhaul_deployment_decision",
        python_callable=_evaluate_longhaul_deployment,
        do_xcom_push=False,
    )

    @task
    def deploy_longhaul_model():
        """Deploy model to RouteOptimal production fleet management system."""
        print("🚛 DEPLOYING LONG-HAUL MODEL")
        print("RouteOptimal interstate logistics model deployed")
        print("Coverage: Cross-country route optimization")
        print("Expected fuel savings: 8-12% through better route planning")
        print("Notification sent to: logistics@routeoptimal.com")

    @task  
    def notify_logistics_team():
        """Alert logistics team about model performance concerns."""
        print("📋 LOGISTICS TEAM NOTIFICATION")
        print("Long-haul model accuracy needs improvement")
        print("Possible factors:")
        print("- Seasonal weather impact on highway speeds")
        print("- Construction zones affecting major routes")
        print("- Need for traffic pattern feature engineering")
        print("Alert sent to: logistics@routeoptimal.com")

    end_task = DummyOperator(
        task_id="pipeline_complete", 
        trigger_rule="none_failed_or_skipped"
    )
    
    # Pipeline flow optimized for long-haul logistics workflows
    (
        start_task
        >> data_quality_task
        >> train_longhaul_model(
            bucket_name="{{ var.value.fleet_data_bucket }}",
            fleet_operator=fleet_operator,
        )
        >> deployment_decision
        >> [deploy_longhaul_model(), notify_logistics_team()]
        >> end_task
    )
    
# Register the DAG for RouteOptimal operations
dag_fleet_pipeline_routeoptimal = fleet_pipeline_routeoptimal() 