import psycopg2

conn = psycopg2.connect("dbname=test user=postgres password=example")

cur = conn.cursor()

# Create tables
cur.execute("""
CREATE TABLE Errors (
    ErrorCode serial PRIMARY KEY,
    Description varchar(255),
    ArrivalDate date,
    ErrorLevel varchar(255),
    FunctionalityCategory varchar(255),
    Source varchar(255)
);
""")

cur.execute("""
CREATE TABLE Programmers (
    ProgrammerCode serial PRIMARY KEY,
    LastName varchar(255),
    FirstName varchar(255),
    Phone varchar(15)
);
""")

cur.execute("""
CREATE TABLE ErrorFixes (
    FixCode serial PRIMARY KEY,
    ErrorCode integer REFERENCES Errors(ErrorCode),
    StartDate date,
    Duration integer,
    ProgrammerCode integer REFERENCES Programmers(ProgrammerCode),
    DailyCost float
);
""")

# Display all critical errors. Sort by error code;
cur.execute("""
SELECT * 
FROM Errors 
WHERE ErrorLevel = 'Critical' 
ORDER BY ErrorCode;
""")

# Count the number of errors of each level (summary query);
cur.execute("""
SELECT ErrorLevel, COUNT(*) 
FROM Errors 
GROUP BY ErrorLevel;
""")

# Calculate the cost of a programmer's work when fixing each error (query with a calculated field);
cur.execute("""
SELECT ef.FixCode, ef.ErrorCode, ef.ProgrammerCode, (ef.Duration * ef.DailyCost) as TotalCost 
FROM ErrorFixes ef;
""")

# Display all errors that came from a given source (query with a parameter);
source_type = 'User' 
cur.execute("""
SELECT * 
FROM Errors 
WHERE Source = %s;
""", (source_type,))

# Count the number of errors that came from users and testers (summary query)
cur.execute("""
SELECT Source, COUNT(*) 
FROM Errors 
WHERE Source IN ('User', 'Tester') 
GROUP BY Source;
""")

# Count the number of critical, important, minor errors fixed by each programmer (cross query);
cur.execute("""
SELECT p.ProgrammerCode, 
       COUNT(CASE WHEN e.ErrorLevel = 'Critical' THEN 1 END) as CriticalCount, 
       COUNT(CASE WHEN e.ErrorLevel = 'Important' THEN 1 END) as ImportantCount, 
       COUNT(CASE WHEN e.ErrorLevel = 'Minor' THEN 1 END) as MinorCount 
FROM Programmers p 
JOIN ErrorFixes ef ON p.ProgrammerCode = ef.ProgrammerCode 
JOIN Errors e ON ef.ErrorCode = e.ErrorCode 
GROUP BY p.ProgrammerCode;
""")

conn.commit()

cur.close()
conn.close()