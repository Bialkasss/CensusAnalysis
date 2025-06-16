# Census Data Analysis Dashboard

A web-based analytics platform for exploring US census data from 1994 using Apache Spark and PySpark on Hadoop clusters. This application provides interactive analysis of demographic and economic patterns through a modern web interface.

## 🚀 Features

- **Real-time Data Analysis**: Distributed processing using Apache Spark on YARN
- **Interactive Dashboard**: Modern web interface with responsive design
- **Multiple Analysis Types**:
  - 📊 Occupation vs Income Analysis
  - 🎓 Education Impact Analysis
  - ⏰ Work Hours Pattern Analysis
  - 👥 Demographics Analysis
  - 🌍 Geographic Income Patterns
  - 🔬 Comprehensive Cross-Analysis
  - 💡 Intelligent Career Recommendations

## 🛠️ Technology Stack

- **Backend**: Python Flask
- **Big Data Processing**: Apache Spark (PySpark)
- **Cluster Management**: Apache Hadoop YARN
- **Data Storage**: HDFS (Hadoop Distributed File System)
- **Frontend**: HTML, CSS, JavaScript with responsive design
- **Containerization**: Docker (optional)

## 📋 Prerequisites

- Apache Hadoop cluster (2.7+ or 3.x)
- Apache Spark (2.4+ or 3.x)
- Python 3.7+
- Java 8 or 11
- YARN Resource Manager running
- HDFS accessible

### Required Python Packages
- Flask
- PySpark
- py4j

## 🔧 Installation & Setup

### 1. Environment Setup

The application runs in a Docker environment with pre-configured Hadoop and Spark clusters. Ensure your Docker container has:

```bash
# Java Environment
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Spark Configuration
export SPARK_HOME=/opt/spark
export PYSPARK_PYTHON=python3

# Hadoop Configuration
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

# Python Path
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH
```

### 2. Data Preparation

Place your census data file in HDFS:
```bash
# Upload data to HDFS
hdfs dfs -put adults_data.csv /opt/code/adults_data.csv
```

**Expected Data Format**: CSV file with the following columns:
- age, workclass, education, marital_status, occupation, relationship, race, sex
- capital_gain, capital_loss, hours_per_week, native_country, income

### 3. Running the Application

Use the provided startup script:

```bash
#!/bin/bash
export PYSPARK_PYTHON=python3
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=/opt/spark
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH

# Install Flask if not already installed
pip3 install Flask

# Start the Flask app
python3 app.py
```

### 4. Access the Dashboard

Once started, access the application at:
```
http://localhost:8888
```
   
## 📊 Application Screenshots

### Main Dashboard
![Main Dashboard](screenshots/dashboard.png)
*Overview of the main analytics dashboard showing key metrics and navigation*

### Occupation Analytics
![Occupation Analysis](screenshots/occupation-analysis.png)
*Occupation-based income analysis and employment patterns*

### *Education Analysis
![Demographic Analysis](screenshots/demographic-analysis.png)
*Detailed education impact on career success*

### Work Hours Analysis
![Demographic Analysis](screenshots/demographic-analysis.png)
*Detailed work-life balance patterns*

### Demographic Analysis
![Demographic Analysis](screenshots/demographic-analysis.png)
*Detailed demographic breakdowns and income correlations*

### Geographic Distribution
![Geographic Analysis](screenshots/geographic-analysis.png)
*Geographic patterns and country-wise income analysis*

### Comprehensive Analysis
![Geographic Analysis](screenshots/geographic-analysis.png)
*Multi-dimensional cross-analysis*

### Recommendations
![Geographic Analysis](screenshots/geographic-analysis.png)
*Career and investment suggestions*


## 📝 License

This project is provided as-is for educational and research purposes.
