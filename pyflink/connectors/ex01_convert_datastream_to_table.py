from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment

# Initialize StreamExecutionEnvironment and configure for BATCH mode
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)

# Create StreamTableEnvironment bound to the execution environment
t_env = StreamTableEnvironment.create(stream_execution_environment=env)

# Create a DataStream with sample data
data = [
    ("Alice", "Engineering", 95000),
    ("Bob", "Engineering", 82000),
    ("Charlie", "Marketing", 71000),
    ("Diana", "Marketing", 78000),
]

type_info = Types.ROW_NAMED(
    ["name", "department", "salary"],
    [Types.STRING(), Types.STRING(), Types.INT()]
)

ds = env.from_collection(collection=data, type_info=type_info)

# Convert DataStream to a Table
table = t_env.from_data_stream(ds)

# Perform SQL / Table API operations
avg_salary_table = table \
    .group_by(table.department) \
    .select(table.department, table.salary.avg.alias("avg_salary"))

# Print the result (executes the pipeline in batch mode)
avg_salary_table.execute().print()

# Register the converted Table as a temporary view. The view is available to
# SQL queries in this TableEnvironment; it does not create a physical table.
t_env.create_temporary_view("employees", table)

# Query the DataStream-backed table using SQL.
avg_salary_sql = t_env.sql_query("""
    SELECT department, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department
    ORDER BY department
""")

print("Average salary queried through the employees view:")
avg_salary_sql.execute().print()
