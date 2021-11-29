#using the Spark SQL Shell

#for new permanent Spark SQL table
#run spark-sql
# >>> CREATE TABLE people (name STRING, age int);
# >>> INSERT INTO people VALUES ("Michael", NULL);
# >>> INSERT INTO people VALUES ("Andy", 30);
# >>> INSERT INTO people VALUES ("Samantha", 19);

#run query
# >>> SELECT * FROM people WHERE age < 20;
# >>> SELECT name FROM people WHERE age IS NULL;
