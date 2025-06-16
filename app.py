from flask import Flask, render_template, jsonify, request
import json
import os

# os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Import your existing CensusDataAnalyzer class
class CensusDataAnalyzer:
    def __init__(self, app_name="Census Data Analysis"):
        # self.spark = SparkSession.builder.appName("CensusAnalysis").config("spark.sql.adaptive.enabled", "true") \
        #     .config("spark.sql.adaptive.coalescePartitions.enabled", "true").getOrCreate()

            
    #Try to fix, make it really run on clusters.
        # self.spark = SparkSession.builder.appName("CensusAnalysis").master("yarn").config("spark.submit.deployMode", "client").config("spark.sql.adaptive.enabled", "true").config("spark.sql.adaptive.coalescePartitions.enabled", "true").config("spark.executor.instances", "2").config("spark.executor.cores", "2").config("spark.executor.memory", "1g").config("spark.driver.memory", "1g").config("spark.dynamicAllocation.enabled", "true").getOrCreate()
        
        self.spark = (SparkSession.builder
                     .appName(app_name)
                     .master("yarn")
                     .config("spark.submit.deployMode", "client")
                     
                     # Resource allocation - adjusted for your single node setup
                     .config("spark.executor.instances", "2")  # Start with 1 for single nodemanager
                     .config("spark.executor.cores", "2")
                     .config("spark.executor.memory", "1g")
                     .config("spark.driver.memory", "1g")
                     .config("spark.driver.cores", "1")
                     
                     # Dynamic allocation settings
                     .config("spark.dynamicAllocation.enabled", "true")
                     .config("spark.dynamicAllocation.minExecutors", "1")
                     .config("spark.dynamicAllocation.maxExecutors", "2")
                     .config("spark.dynamicAllocation.initialExecutors", "2")
                     
                     # SQL optimization
                     .config("spark.sql.adaptive.enabled", "true")
                     .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                     .config("spark.sql.adaptive.skewJoin.enabled", "true")
                     
                     # YARN specific configurations
                     .config("spark.yarn.queue", "default")
                     .config("spark.yarn.am.memory", "512m")
                     .config("spark.yarn.am.cores", "1")
                     
                     # Serialization and network
                     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                     .config("spark.network.timeout", "300s")
                     .config("spark.executor.heartbeatInterval", "20s")
                     
                     # Shuffle and storage
                     .config("spark.sql.shuffle.partitions", "4")  # Reduced for single node
                     .config("spark.default.parallelism", "4")
                     
                     .getOrCreate())
        
        # Define schema for census data
        self.schema = StructType([
            StructField("age", IntegerType(), True),
            StructField("workclass", StringType(), True),
            StructField("education", StringType(), True),
            StructField("marital_status", StringType(), True),
            StructField("occupation", StringType(), True),
            StructField("relationship", StringType(), True),
            StructField("race", StringType(), True),
            StructField("sex", StringType(), True),
            StructField("capital_gain", IntegerType(), True),
            StructField("capital_loss", IntegerType(), True),
            StructField("hours_per_week", IntegerType(), True),
            StructField("native_country", StringType(), True),
            StructField("income", StringType(), True)
        ])
        
        print("=== Spark Cluster Information ===")
        self.print_cluster_info()
        

        def simple_cluster_check(spark):
            """Simple cluster check for initialization"""
            sc = spark.sparkContext
            print(f"\n=== Spark Cluster Info ===")
            print(f"Master: {sc.master}")
            print(f"App Name: {sc.appName}")
            print(f"Default Parallelism: {sc.defaultParallelism}")
            
            # Quick cluster check
            if "yarn" in sc.master.lower():
                print("✓ YARN cluster mode detected")
            elif "local" in sc.master.lower():
                print("⚠ LOCAL mode - not distributed")
            
            # Test if we can distribute work
            try:
                test_rdd = sc.parallelize([1, 2, 3, 4], 2)
                result = test_rdd.map(lambda x: x * 2).collect()
                print(f"✓ Basic RDD operations working: {result}")
            except Exception as e:
                print(f"✗ RDD operations failed: {e}")
            print("========================\n")

        simple_cluster_check(self.spark)
        
    def print_cluster_info(self):
        """Print detailed information about the Spark cluster"""
        try:
            sc = self.spark.sparkContext
            print(f"Application Name: {sc.appName}")
            print(f"Master: {sc.master}")
            print(f"Spark Version: {sc.version}")
            print(f"Application ID: {sc.applicationId}")
            print(f"Default Parallelism: {sc.defaultParallelism}")
            print(f"Spark UI: {sc.uiWebUrl}")
            
            # Get executor information
            executor_infos = sc.statusTracker().getExecutorInfos()
            print(f"Number of Executors: {len(executor_infos)}")
            
            for i, executor in enumerate(executor_infos):
                print(f"  Executor {i}: {executor.host}:{executor.port}")
                print(f"    Cores: {executor.totalCores}")
                print(f"    Memory: {executor.maxMemory / (1024**3):.2f} GB")
            
        except Exception as e:
            print(f"Could not retrieve cluster info: {e}")
    
    def verify_yarn_connection(self):
        """Verify connection to YARN cluster"""
        try:
            # Test basic RDD operation to verify cluster connectivity
            test_rdd = self.spark.sparkContext.parallelize([1, 2, 3, 4, 5], 2)
            result = test_rdd.map(lambda x: x * 2).collect()
            print(f"✓ YARN cluster test successful: {result}")
            
            # Check available partitions
            print(f"✓ Test RDD partitions: {test_rdd.getNumPartitions()}")
            
        except Exception as e:
            print(f"✗ YARN cluster test failed: {e}")
            print("Check your Hadoop/YARN configuration and cluster status")
    
        
    def load_data(self, path):
        """Load census data"""
        df = self.spark.read.csv(
            path,
            header=False, schema=self.schema
        ).na.replace(" ?", None).dropna()
        
        df_clean = df.withColumn(
            "high_income", 
            when(col("income").contains(">50K"), 1).otherwise(0)
        )
        print(f"✓ Data loaded successfully")
        print(f"  Records: {df_clean.count()}")
        print(f"  Columns: {len(df_clean.columns)}")
        print(f"  Partitions: {df_clean.rdd.getNumPartitions()}")
        return df_clean
    
    def get_cluster_metrics(self):
        """Get cluster performance metrics"""
        try:
            sc = self.spark.sparkContext
            status_tracker = sc.statusTracker()
            
            print("\n=== Cluster Metrics ===")
            print(f"Active Jobs: {len(status_tracker.getActiveJobIds())}")
            print(f"Active Stages: {len(status_tracker.getActiveStageIds())}")
            
            # Executor metrics
            executors = status_tracker.getExecutorInfos()
            total_cores = sum(ex.totalCores for ex in executors)
            total_memory = sum(ex.maxMemory for ex in executors)
            
            print(f"Total Executor Cores: {total_cores}")
            print(f"Total Executor Memory: {total_memory / (1024**3):.2f} GB")
            
        except Exception as e:
            print(f"Could not retrieve metrics: {e}")
            
    def occupation_analysis(self, df, n=20):
        """Analyze occupation vs income patterns"""
        occupation_stats = df.groupBy("occupation").agg(
            count("*").alias("total_count"),
            avg("age").alias("avg_age"),
            avg("hours_per_week").alias("avg_hours"),
            (sum("high_income") / count("*") * 100).alias("high_income_rate"),
            sum(when(col("sex") == "Male", 1).otherwise(0)).alias("male_count"),
            sum(when(col("sex") == "Female", 1).otherwise(0)).alias("female_count")
        ).select(
            "occupation",
            "total_count",
            "avg_age", 
            "avg_hours",
            "high_income_rate",
            "male_count",
            "female_count"
        ).orderBy(desc("high_income_rate"), "occupation")
    
        return occupation_stats.limit(n).collect()

    def education_analysis(self, df, n=20):
        """Analyze education impact on income and career"""
        education_stats = df.groupBy("education").agg(
            count("*").alias("total_count"),
            avg("age").alias("avg_age"),
            avg("hours_per_week").alias("avg_hours"),
            (sum("high_income") / count("*") * 100).alias("high_income_rate")
        ).orderBy(desc("high_income_rate"))
        
        edu_occupation = df.groupBy("education", "occupation").agg(
            count("*").alias("count"),
            (sum("high_income") / count("*") * 100).alias("high_income_rate")
        ).filter(col("count") >= 10).orderBy(desc("high_income_rate"))
        
        return education_stats.limit(n).collect(), edu_occupation.limit(n).collect()

    def work_hours_analysis(self, df, n=20):
        """Analyze work hours patterns"""
        df_hours = df.withColumn(
            "hour_range",
            when(col("hours_per_week") < 20, "Part-time (<20h)")
            .when(col("hours_per_week") < 40, "Reduced (20-39h)")
            .when(col("hours_per_week") <= 45, "Full-time (40-45h)")
            .otherwise("Overtime (>45h)")
        )
        
        hours_stats = df_hours.groupBy("hour_range").agg(
            count("*").alias("total_count"),
            avg("age").alias("avg_age"),
            avg("hours_per_week").alias("avg_hours"),
            (sum("high_income") / count("*") * 100).alias("high_income_rate"),
            sum(when(col("sex") == "Male", 1).otherwise(0)).alias("male_count"),
            sum(when(col("sex") == "Female", 1).otherwise(0)).alias("female_count")
        ).orderBy("avg_hours")
        
        return hours_stats.collect()

    def demographics_analysis(self, df, n=20):
        """Analyze demographic patterns"""
        gender_stats = df.groupBy("sex").agg(
            count("*").alias("total_count"),
            avg("age").alias("avg_age"),
            avg("hours_per_week").alias("avg_hours"),
            (sum("high_income") / count("*") * 100).alias("high_income_rate")
        ).collect()
        
        marital_stats = df.groupBy("marital_status").agg(
            count("*").alias("total_count"),
            avg("age").alias("avg_age"),
            (sum("high_income") / count("*") * 100).alias("high_income_rate"),
            sum(when(col("sex") == "Male", 1).otherwise(0)).alias("male_count"),
            sum(when(col("sex") == "Female", 1).otherwise(0)).alias("female_count")
        ).orderBy(desc("high_income_rate")).collect()
        
        df_age = df.withColumn(
            "age_group",
            when(col("age") < 25, "Young (18-24)")
            .when(col("age") < 35, "Early Career (25-34)")
            .when(col("age") < 45, "Mid Career (35-44)")
            .when(col("age") < 55, "Senior (45-54)")
            .otherwise("Mature (55+)")
        )
        
        age_stats = df_age.groupBy("age_group").agg(
            count("*").alias("total_count"),
            avg("hours_per_week").alias("avg_hours"),
            (sum("high_income") / count("*") * 100).alias("high_income_rate")
        ).orderBy("total_count").collect()
        
        return gender_stats, marital_stats, age_stats

    def geographic_analysis(self, df, n=20):
        """Analyze geographic patterns"""
        geo_stats = df.filter(col("native_country") != "?").groupBy("native_country").agg(
            count("*").alias("total_count"),
            avg("age").alias("avg_age"),
            avg("hours_per_week").alias("avg_hours"),
            (sum("high_income") / count("*") * 100).alias("high_income_rate")
        ).filter(col("total_count") >= 10).orderBy(desc("high_income_rate"))
        
        return geo_stats.limit(n).collect()

    def comprehensive_analysis(self, df, n=20):
        """Run comprehensive cross-analysis"""
        comprehensive_stats = df.groupBy("sex", "education", "occupation").agg(
            count("*").alias("total_count"),
            avg("age").alias("avg_age"),
            avg("hours_per_week").alias("avg_hours"),
            (sum("high_income") / count("*") * 100).alias("high_income_rate")
        ).filter(col("total_count") >= 5).orderBy(desc("high_income_rate"))
        
        return comprehensive_stats.limit(n).collect()

    def generate_recommendations(self, df, n=20):
        """Generate intelligent recommendations"""
        career_recs = df.groupBy("education", "occupation").agg(
            count("*").alias("count"),
            (sum("high_income") / count("*") * 100).alias("success_rate"),
            avg("hours_per_week").alias("avg_hours")
        ).filter(col("count") >= 20).orderBy(desc("success_rate"))
        
        balance_recs = df.filter(col("high_income") == 1).groupBy("occupation").agg(
            avg("hours_per_week").alias("avg_hours"),
            count("*").alias("high_earners")
        ).filter(col("high_earners") >= 10).orderBy("avg_hours")
        
        capital_analysis = df.filter(col("capital_gain") > 0).groupBy("occupation").agg(
            avg("capital_gain").alias("avg_capital_gain"),
            count("*").alias("people_with_gains"),
            avg("capital_loss").alias("avg_capital_loss")
        ).orderBy(desc("avg_capital_gain"))
        
        return career_recs.limit(n).collect(), balance_recs.limit(n).collect(), capital_analysis.limit(n).collect()

    def stop(self):
        self.spark.stop()

# Flask application
app = Flask(__name__)

# Global analyzer instance
analyzer = None
df = None

def init_analyzer(data_path):
    global analyzer, df
    if analyzer is None:
        analyzer = CensusDataAnalyzer()
        df = analyzer.load_data(data_path)
    return analyzer, df

def row_to_dict(row):
    """Convert Spark Row to dictionary for JSON serialization"""
    return {field: float(value) if isinstance(value, (int, float)) else value 
            for field, value in row.asDict().items()}

@app.route('/')
def index():
    DATA_PATH = "hdfs://hadoop-master:9000/opt/code/adults_data.csv"
    print("initializing data")
    analyzer, data = init_analyzer(DATA_PATH)
    return render_template('index.html')

@app.route('/api/analyze/<analysis_type>')
def analyze(analysis_type):
    try:
        # Initialize analyzer with your HDFS data path
        DATA_PATH = "hdfs://hadoop-master:9000/opt/code/adults_data.csv" 
        print("anallyzer") 
        analyzer, data = init_analyzer(DATA_PATH)
       
        n = request.args.get('n', 20, type=int)

        if analysis_type == 'occupation':
            results = analyzer.occupation_analysis(data, n)
            return jsonify({
                'title': 'Occupation Analysis',
                'description': 'Analysis of occupation vs income patterns',
                'data': [row_to_dict(row) for row in results]
            })
            
        elif analysis_type == 'education':
            education_stats, edu_occupation = analyzer.education_analysis(data, n)
            return jsonify({
                'title': 'Education Analysis',
                'description': 'Analysis of education impact on income',
                'education_data': [row_to_dict(row) for row in education_stats],
                'education_occupation_data': [row_to_dict(row) for row in edu_occupation]
            })
            
        elif analysis_type == 'work_hours':
            results = analyzer.work_hours_analysis(data, n)
            return jsonify({
                'title': 'Work Hours Analysis',
                'description': 'Analysis of work hours patterns',
                'data': [row_to_dict(row) for row in results]
            })
            
        elif analysis_type == 'demographics':
            gender_stats, marital_stats, age_stats = analyzer.demographics_analysis(data, n)
            return jsonify({
                'title': 'Demographics Analysis',
                'description': 'Analysis of demographic patterns',
                'gender_data': [row_to_dict(row) for row in gender_stats],
                'marital_data': [row_to_dict(row) for row in marital_stats],
                'age_data': [row_to_dict(row) for row in age_stats]
            })
            
        elif analysis_type == 'geographic':
            results = analyzer.geographic_analysis(data, n)
            return jsonify({
                'title': 'Geographic Analysis',
                'description': 'Analysis of geographic income patterns',
                'data': [row_to_dict(row) for row in results]
            })
            
        elif analysis_type == 'comprehensive':
            results = analyzer.comprehensive_analysis(data, n)
            return jsonify({
                'title': 'Comprehensive Cross-Analysis',
                'description': 'Multi-dimensional analysis combining multiple factors',
                'data': [row_to_dict(row) for row in results]
            })
            
        elif analysis_type == 'recommendations':
            career_recs, balance_recs, capital_analysis = analyzer.generate_recommendations(data, n)
            return jsonify({
                'title': 'Intelligent Recommendations',
                'description': 'Career and investment recommendations based on data',
                'career_data': [row_to_dict(row) for row in career_recs],
                'balance_data': [row_to_dict(row) for row in balance_recs],
                'capital_data': [row_to_dict(row) for row in capital_analysis]
            })
            
        else:
            return jsonify({'error': 'Invalid analysis type'}), 400
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dataset_info')
def dataset_info():
    try:
        if df is not None:
            total_records = df.count()
            schema_info = [{'name': field.name, 'type': str(field.dataType)} for field in df.schema.fields]
            return jsonify({
                'total_records': total_records,
                'schema': schema_info
            })
        else:
            return jsonify({'error': 'Dataset not loaded'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8888)