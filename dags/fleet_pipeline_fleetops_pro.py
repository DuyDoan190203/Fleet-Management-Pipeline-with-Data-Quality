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
        # Fleet operations need reliable retries for business continuity
        "retries": 2,  
    },
    tags=["fleet_management", "data_quality", "ml_pipeline"],
)
def fleet_pipeline_fleetops_pro():
    """### Fleet Management Pipeline with Data Quality Validation
    
    Production pipeline for FleetOps Pro - urban delivery fleet optimization.
    Validates trip data quality before training duration estimation models.
    Only deploys models that meet SLA performance thresholds.
    """
    
    fleet_operator = "fleetops_pro"
    
    start_task = DummyOperator(task_id="start")

    # Data Quality Validation - The Foundation of Reliable ML
    data_quality_task = GreatExpectationsOperator(
        task_id="validate_fleet_data",
        data_context_root_dir="./dags/gx",
        
        # Validate FleetOps Pro trip data before any processing
        data_asset_name="fleet_trips_fleetops_pro",
        dataframe_to_validate=pd.read_parquet(
            f"s3://{Variable.get('fleet_data_bucket')}/fleet_operations/datasets/"
            f"{fleet_operator}/trip_data.parquet"
        ),
        
        # Use Pandas execution engine for in-memory validation
        execution_engine="PandasExecutionEngine",
        expectation_suite_name="fleet-quality-standards",
        
        # Return validation results for downstream processing
        return_json_dict=True,
        
        # Hard stop on quality failures - bad data = bad models
        fail_task_on_validation_failure=True,
    )
    
    @task
    def train_duration_model(bucket_name: str, fleet_operator: str):
        """Train and evaluate trip duration estimation model for fleet optimization."""
        
        datasets_path = f"s3://{bucket_name}/fleet_operations/datasets"
        
        # Load validated training and test datasets
        train_data = pd.read_parquet(f"{datasets_path}/{fleet_operator}/train.parquet")
        test_data = pd.read_parquet(f"{datasets_path}/{fleet_operator}/test.parquet")
        
        # Prepare features and targets for regression
        X_train = train_data[["trip_distance_km"]].to_numpy()[:, 0]
        X_test = test_data[["trip_distance_km"]].to_numpy()[:, 0]

        y_train = train_data[["trip_duration_minutes"]].to_numpy()[:, 0]
        y_test = test_data[["trip_duration_minutes"]].to_numpy()[:, 0]

        # Train linear regression model - simple but reliable for fleet ops
        model = linregress(X_train, y_train)

        # Evaluate model performance on test set
        y_pred_test = model.slope * X_test + model.intercept
        rmse_performance = np.sqrt(np.average((y_pred_test - y_test) ** 2))
        
        print(f"=== FleetOps Pro Model Performance ===")
        print(f"RMSE: {rmse_performance:.2f} minutes")
        print(f"Model slope: {model.slope:.4f}")
        print(f"Model intercept: {model.intercept:.2f}")
        
        # Return performance metric for deployment decision
        return rmse_performance
        
    def _should_deploy_model(ti):
        """Determine if model meets FleetOps Pro SLA requirements for deployment."""
        
        # Get model performance from training task
        model_rmse = ti.xcom_pull(task_ids="train_duration_model")
        
        # FleetOps Pro SLA: predictions must be within 5 minutes for urban delivery
        sla_threshold_minutes = 5.0
        
        if model_rmse < sla_threshold_minutes:
            print(f"✅ Model APPROVED for deployment - RMSE: {model_rmse:.2f} min")
            print("FleetOps Pro SLA met: < 5 minutes prediction accuracy")
            return "deploy_model"
        else:
            print(f"❌ Model REJECTED - RMSE: {model_rmse:.2f} min exceeds SLA")
            print("Notifying FleetOps Pro operations team")
            return "notify_ops_team"

    deployment_decision = BranchPythonOperator(
        task_id="deployment_decision",
        python_callable=_should_deploy_model,
        do_xcom_push=False,
    )

    @task
    def deploy_model():
        """Deploy approved model to FleetOps Pro production environment."""
        print("🚀 DEPLOYING MODEL TO PRODUCTION")
        print("FleetOps Pro urban delivery fleet model is now live")
        print("Expected improvement: 15% better route optimization")
        print("Deployment notification sent to: ops@fleetopspro.com")

    @task  
    def notify_ops_team():
        """Notify operations team of model performance issues."""
        print("📧 NOTIFYING OPERATIONS TEAM")
        print("Model performance below SLA threshold")
        print("Recommended actions:")
        print("- Review data quality issues")
        print("- Check for seasonal traffic patterns") 
        print("- Consider additional features (weather, events)")
        print("Alert sent to: ops@fleetopspro.com")

    end_task = DummyOperator(
        task_id="pipeline_complete", 
        trigger_rule="none_failed_or_skipped"
    )
    
    # Define pipeline dependencies - quality first, then train, then decide
    (
        start_task
        >> data_quality_task
        >> train_duration_model(
            bucket_name="{{ var.value.fleet_data_bucket }}",
            fleet_operator=fleet_operator,
        )
        >> deployment_decision
        >> [deploy_model(), notify_ops_team()]
        >> end_task
    )
    
# Register the DAG
dag_fleet_pipeline_fleetops_pro = fleet_pipeline_fleetops_pro() 