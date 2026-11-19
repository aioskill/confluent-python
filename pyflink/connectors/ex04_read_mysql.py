from pyflink.table import EnvironmentSettings, TableEnvironment

"""
Prepare MYSQL database

Launch mysql in sa01 node using root access
$ sudo mysql

CREATE DATABASE training;
USE training;

CREATE TABLE employees (id INT primary key, name varchar(255), salary DOUBLE, dept_id INT);
INSERT INTO employees (id, name, salary, dept_id) VALUES (1, 'test 1', 200, 5), (2, 'test 2', 300, 5);


# Create user to connect from flink
CREATE USER IF NOT EXISTS 'training'@'%' IDENTIFIED BY 'training321';
GRANT ALL PRIVILEGES ON training.* TO 'training'@'%';
FLUSH PRIVILEGES;

"""


def read_from_jdbc():
    # Setup environment
    env_settings = EnvironmentSettings.in_batch_mode()
    table_env = TableEnvironment.create(env_settings)

    mysql_user = "training"
    mysql_password =  "training321"
    mysql_url = "jdbc:mysql://sa01:3306/training?useSSL=false&allowPublicKeyRetrieval=true"


    # Create JDBC source. The connector JARs are installed by
    # flinklibs/configure; credentials can be overridden without editing code.
    table_env.execute_sql(f"""
    CREATE TABLE employees (
        id INT,
        name STRING,
        salary DOUBLE, 
        dept_id INT
    ) WITH (
        'connector' = 'jdbc',
        'url' = '{mysql_url}',
        'table-name' = 'employees',
        'username' = '{mysql_user}',
        'password' = '{mysql_password}'

    )
    """)

    # 'scan.partition.column' = 'id',
    # 'scan.partition.num' = '4',
    # 'scan.fetch-size' = '1000'
    # 'scan.partition.lower-bound': '0'
    # 'scan.partition.upper-bound': '1000000'

    # Query with aggregation
    result = table_env.sql_query("""
            SELECT dept_id, AVG(salary) as avg_salary
            FROM employees
            GROUP BY dept_id
            HAVING AVG(salary)>100
            ORDER BY dept_id
    """)

    # Execute and print
    result.execute().print()


if __name__ == '__main__':
    read_from_jdbc()
