import psycopg2

conn = psycopg2.connect("dbname=test user=postgres password=example")

cur = conn.cursor()

# Створюємо таблиці
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

# Відображаємо всі критичні помилки. Сортуємо за кодом помилки;
cur.execute("""
SELECT * 
FROM Errors 
WHERE ErrorLevel = 'Critical' 
ORDER BY ErrorCode;
""")

# Рахуємо кількість помилок кожного рівня (підсумковий запит);
cur.execute("""
SELECT ErrorLevel, COUNT(*) 
FROM Errors 
GROUP BY ErrorLevel;
""")

# Розраховуємо вартість роботи програміста при виправленні кожної помилки (запит з обчислювальним полем);
cur.execute("""
SELECT ef.FixCode, ef.ErrorCode, ef.ProgrammerCode, (ef.Duration * ef.DailyCost) as TotalCost 
FROM ErrorFixes ef;
""")

# Відображаємо всі помилки, що надійшли з заданого джерела (запит з параметром);
source_type = 'User' 
cur.execute("""
SELECT * 
FROM Errors 
WHERE Source = %s;
""", (source_type,))

# Рахуємо кількість помилок, що надійшли від користувачів та тестерів (підсумковий запит)
cur.execute("""
SELECT Source, COUNT(*) 
FROM Errors 
WHERE Source IN ('User', 'Tester') 
GROUP BY Source;
""")

# Рахуємо кількість критичних, важливих, незначних помилок, виправлених кожним програмістом (перехресний запит);
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
