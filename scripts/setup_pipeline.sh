#!/bin/bash

# Fleet Management Pipeline Setup Script
# Sets up the complete environment for running the fleet management data pipeline

set -e  # Exit on any error

echo "🚛 Fleet Management Pipeline Setup"
echo "=================================="

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed"
    exit 1
fi

echo "✅ Python 3 found: $(python3 --version)"

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "📋 Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Setup Great Expectations
echo "🔍 Initializing Great Expectations..."
if [ ! -d "great_expectations" ]; then
    great_expectations init
    echo "✅ Great Expectations initialized"
else
    echo "✅ Great Expectations already configured"
fi

# Create necessary directories
echo "📁 Creating project directories..."
mkdir -p data/fleet_operations/datasets/{fleetops_pro,routeoptimal,logiflow}
mkdir -p logs
mkdir -p models
mkdir -p reports

# Setup Airflow (basic configuration)
echo "🌪️ Setting up Airflow..."
export AIRFLOW_HOME=$(pwd)/airflow
if [ ! -d "$AIRFLOW_HOME" ]; then
    mkdir -p $AIRFLOW_HOME
    # Initialize Airflow database
    airflow db init
    
    # Create admin user
    airflow users create \
        --username admin \
        --firstname Fleet \
        --lastname Admin \
        --role Admin \
        --email admin@fleetops.com \
        --password admin123
    
    echo "✅ Airflow initialized with admin/admin123"
else
    echo "✅ Airflow already configured"
fi

# Copy DAGs to Airflow DAGs folder
echo "📄 Setting up Airflow DAGs..."
cp -r dags/* $AIRFLOW_HOME/dags/ 2>/dev/null || echo "DAGs will be copied when available"

# Setup environment variables
echo "🔐 Setting up environment variables..."
cat > .env << EOF
# Fleet Management Pipeline Environment Variables
FLEET_DATA_BUCKET=fleet-operations-data
AWS_DEFAULT_REGION=us-west-2
AIRFLOW_HOME=$(pwd)/airflow

# Fleet Operator Configurations
FLEETOPS_PRO_SLA_THRESHOLD=5.0
ROUTEOPTIMAL_SLA_THRESHOLD=30.0
LOGIFLOW_SLA_THRESHOLD=2.0

# Notification Settings
FLEETOPS_PRO_EMAIL=ops@fleetopspro.com
ROUTEOPTIMAL_EMAIL=logistics@routeoptimal.com
LOGIFLOW_EMAIL=delivery@logiflow.com
EOF

echo "✅ Environment variables configured in .env"

# Generate sample data (for demo purposes)
echo "📊 Generating sample fleet data..."
python3 -c "
import pandas as pd
import numpy as np
np.random.seed(42)

def generate_sample_data(fleet_type, n=1000):
    if fleet_type == 'fleetops_pro':
        distances = np.random.exponential(5, n)
        durations = distances * 2.5 + np.random.normal(8, 3, n)
    elif fleet_type == 'routeoptimal':
        distances = np.random.normal(450, 150, n)
        distances = np.clip(distances, 100, 800)
        durations = distances * 0.75 + np.random.normal(45, 15, n)
    else:  # logiflow
        distances = np.random.gamma(2, 1.5, n)
        durations = distances * 3.0 + np.random.normal(5, 2, n)
    
    df = pd.DataFrame({
        'trip_id': [f'{fleet_type}_{i:06d}' for i in range(n)],
        'trip_distance_km': distances,
        'trip_duration_minutes': durations
    })
    
    # Split into train/test
    train_df = df.iloc[:800]
    test_df = df.iloc[800:]
    
    train_df.to_parquet(f'data/fleet_operations/datasets/{fleet_type}/train.parquet')
    test_df.to_parquet(f'data/fleet_operations/datasets/{fleet_type}/test.parquet')
    df.to_parquet(f'data/fleet_operations/datasets/{fleet_type}/trip_data.parquet')
    print(f'Generated {len(df)} samples for {fleet_type}')

for fleet in ['fleetops_pro', 'routeoptimal', 'logiflow']:
    generate_sample_data(fleet)

print('Sample data generation completed')
"

echo "✅ Sample data generated for all fleet operators"

# Setup monitoring (basic)
echo "📈 Setting up monitoring..."
mkdir -p monitoring
cat > monitoring/health_check.py << 'EOF'
#!/usr/bin/env python3
"""Basic health check for fleet management pipeline."""

import os
import sys
from pathlib import Path

def check_airflow():
    """Check if Airflow is properly configured."""
    airflow_home = os.environ.get('AIRFLOW_HOME')
    if not airflow_home or not Path(airflow_home).exists():
        return False, "Airflow home directory not found"
    return True, "Airflow configuration OK"

def check_data():
    """Check if sample data exists."""
    data_path = Path("data/fleet_operations/datasets")
    fleet_operators = ["fleetops_pro", "routeoptimal", "logiflow"]
    
    for operator in fleet_operators:
        operator_path = data_path / operator
        if not operator_path.exists():
            return False, f"Data directory for {operator} not found"
        
        required_files = ["train.parquet", "test.parquet", "trip_data.parquet"]
        for file in required_files:
            if not (operator_path / file).exists():
                return False, f"Missing {file} for {operator}"
    
    return True, "All data files present"

def main():
    """Run health checks."""
    print("🏥 Fleet Management Pipeline Health Check")
    print("=" * 50)
    
    checks = [
        ("Airflow Configuration", check_airflow),
        ("Sample Data", check_data),
    ]
    
    all_passed = True
    for name, check_func in checks:
        try:
            passed, message = check_func()
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"{name}: {status} - {message}")
            if not passed:
                all_passed = False
        except Exception as e:
            print(f"{name}: ❌ ERROR - {str(e)}")
            all_passed = False
    
    print("\n" + "=" * 50)
    if all_passed:
        print("🎉 All health checks passed! Pipeline is ready.")
        sys.exit(0)
    else:
        print("⚠️  Some health checks failed. Please review setup.")
        sys.exit(1)

if __name__ == "__main__":
    main()
EOF

chmod +x monitoring/health_check.py

echo "✅ Monitoring setup completed"

# Final setup validation
echo ""
echo "🔍 Running setup validation..."
python3 monitoring/health_check.py

echo ""
echo "🎉 Fleet Management Pipeline Setup Complete!"
echo ""
echo "Next steps:"
echo "1. source venv/bin/activate"
echo "2. export AIRFLOW_HOME=$(pwd)/airflow"  
echo "3. airflow standalone  # Start Airflow"
echo "4. Open http://localhost:8080 (admin/admin123)"
echo "5. jupyter notebook  # For data analysis"
echo ""
echo "Pipeline is ready for fleet operations! 🚛" 