import psycopg2

conn = psycopg2.connect("dbname=test user=postgres password=example")

cur = conn.cursor()

# Створити таблиці
cur.execute("""
CREATE TABLE Clients (
    ClientID serial PRIMARY KEY,
    CompanyName varchar(255),
    AccountNumber varchar(255),
    Phone varchar(15),
    ContactPerson varchar(255),
    Address varchar(255)
);
""")

cur.execute("""
CREATE TABLE Cars (
    CarID serial PRIMARY KEY,
    CarBrand varchar(255),
    NewCarCost float,
    ClientID integer REFERENCES Clients(ClientID)
);
""")

cur.execute("""
CREATE TABLE Repair (
    RepairID serial PRIMARY KEY,
    RepairStartDate date,
    CarID integer REFERENCES Cars(CarID),
    RepairType varchar(255),
    HourlyCost float,
    Discount float,
    HoursNeeded integer
);
""")

# Вставити дані
cur.execute("""
INSERT INTO Clients (CompanyName, AccountNumber, Phone, ContactPerson, Address) 
VALUES 
('ABC Corp', '123456', '+1234567890', 'John Doe', '123 Street, City, Country');
""")

cur.execute("""
INSERT INTO Cars (CarBrand, NewCarCost, ClientID) 
VALUES 
('Toyota', 30000, 1);
""")

cur.execute("""
INSERT INTO Repair (RepairStartDate, CarID, RepairType, HourlyCost, Discount, HoursNeeded) 
VALUES 
('2022-01-01', 1, 'major', 50, 0.1, 10);
""")

# Відобразити інформацію про всі гарантійні ремонти. Відсортувати назви клієнтів за алфавітом;
cur.execute("""
SELECT c.CompanyName, r.*
FROM Repair r
JOIN Cars car ON r.CarID = car.CarID
JOIN Clients c ON car.ClientID = c.ClientID
WHERE r.RepairType = 'warranty'
ORDER BY c.CompanyName;
""")

# Порахувати вартість ремонту, та вартість з урахуванням знижки, для кожного автомобіля (запит з обчислювальним полем);
cur.execute("""
SELECT car.CarID, SUM(r.HourlyCost * r.HoursNeeded) AS TotalCost, SUM(r.HourlyCost * r.HoursNeeded * (1 - r.Discount)) AS DiscountedCost
FROM Repair r
JOIN Cars car ON r.CarID = car.CarID
GROUP BY car.CarID;
""")

# Відобразити інформацію по ремонту для всіх авто заданої марки (запит з параметром);
car_brand = 'fiesta'
cur.execute("""
SELECT r.*
FROM Repair r
JOIN Cars car ON r.CarID = car.CarID
WHERE car.CarBrand = %s;
""", (car_brand,))

# Порахувати загальну суму, яку сплатив кожен клієнт (підсумковий запит);
cur.execute("""
SELECT c.ClientID, SUM(r.HourlyCost * r.HoursNeeded * (1 - r.Discount)) AS TotalPaid
FROM Repair r
JOIN Cars car ON r.CarID = car.CarID
JOIN Clients c ON car.ClientID = c.ClientID
GROUP BY c.ClientID;
""")

# Порахувати кількість кожного типу ремонтів для кожного клієнта (перехресний запит);
cur.execute("""
SELECT c.ClientID, 
       COUNT(CASE WHEN r.RepairType = 'warranty' THEN 1 END) AS WarrantyCount, 
       COUNT(CASE WHEN r.RepairType = 'scheduled' THEN 1 END) AS ScheduledCount, 
       COUNT(CASE WHEN r.RepairType = 'major' THEN 1 END) AS MajorCount
FROM Repair r
JOIN Cars car ON r.CarID = car.CarID
JOIN Clients c ON car.ClientID = c.ClientID
GROUP BY c.ClientID;
""")

# Порахувати кількість ремонтів для кожної марки автомобіля.
cur.execute("""
SELECT car.CarBrand, COUNT(r.RepairID) AS RepairCount
FROM Repair r
JOIN Cars car ON r.CarID = car.CarID
GROUP BY car.CarBrand;
""")

conn.commit()

cur.close()
conn.close()

