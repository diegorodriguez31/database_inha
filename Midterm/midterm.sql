-- Create databade
from sqlalchemy import create_engine
my_conn = create_engine("sqlite:////content/ITS.db")
conn = my_conn.connect()

-- Create table Vehicle
CREATE TABLE Vehicle(
   ID_vehicle INTEGER PRIMARY KEY AUTOINCREMENT,
   Registration_id CHAR(8) NOT NULL,
   Manufacturer VARCHAR(25) NOT NULL,
   Model VARCHAR(30) NOT NULL,
   Color VARCHAR(15) NOT NULL,
   Current_odometer REAL NOT NULL,
   Passenger_capacity SMALLINT NOT NULL,
   Unavailable LOGICAL NOT NULL,
   UNIQUE(Registration_id)
);

-- Create table Country
CREATE TABLE Country(
   ID_country INTEGER PRIMARY KEY AUTOINCREMENT,
   Name VARCHAR(20) NOT NULL
);

-- Create table _Language
CREATE TABLE _Language(
   Code CHAR(2),
   PRIMARY KEY(Code)
);

-- Create table Official
CREATE TABLE Official(
   ID_city_official CHAR(8),
   Name VARCHAR(20) NOT NULL,
   Role VARCHAR(30) NOT NULL,
   ID_country INT NOT NULL,
   Code CHAR(2) NOT NULL,
   PRIMARY KEY(ID_city_official),
   FOREIGN KEY(ID_country) REFERENCES Country(ID_country),
   FOREIGN KEY(Code) REFERENCES _Language(Code)
);

-- Create table Driver
CREATE TABLE Driver(
   License_number CHAR(18),
   Name VARCHAR(20) NOT NULL,
   Clearance_level BYTE NOT NULL,
   FATL_level BYTE,
   FATL_qualification_date DATE,
   STLVT_level BYTE,
   STLVT_qualification_date DATE,
   STLVT_certifying_authority VARCHAR(30),
   PRIMARY KEY(License_number),
   CONSTRAINT CHECK_Clearance_Level CHECK (Clearance_level > 0 AND Clearance_level < 5), 
   CONSTRAINT CHECK_FATL_Level CHECK (FATL_level > 0 AND FATL_level < 11),
   CONSTRAINT CHECK_STLVT_Level CHECK (STLVT_level > 0 and STLVT_level < 6),
   CONSTRAINT CHECK_FATL_Information_Consistency CHECK(
    (FATL_level IS NULL AND FATL_qualification_date IS NULL)
    OR (FATL_level IS NOT NULL AND FATL_qualification_date IS NOT NULL)),
   CONSTRAINT CHECK_STLVT_Information_Consistency CHECK(
    (STLVT_level IS NULL AND STLVT_qualification_date IS NULL AND STLVT_certifying_authority IS NULL)
    OR (STLVT_level IS NOT NULL AND STLVT_qualification_date IS NOT NULL AND STLVT_certifying_authority IS NOT NULL))
);

-- Create table Booking
CREATE TABLE Booking(
   Booking_reference_number INTEGER PRIMARY KEY AUTOINCREMENT,
   Pick_up_location_name VARCHAR(30) NOT NULL,
   Pick_up_location_street VARCHAR(50) NOT NULL,
   Pick_up_location_street_number INT NOT NULL,
   Pick_up_location_city VARCHAR(20) NOT NULL,
   Pick_up_location_type VARCHAR(15) NOT NULL,
   Drop_off_location_name VARCHAR(30) NOT NULL,
   Drop_off_location_street VARCHAR(50) NOT NULL,
   Drop_off_location_street_number INT NOT NULL,
   Drop_off_location_city VARCHAR(20) NOT NULL,
   Drop_off_location_type VARCHAR(15) NOT NULL,
   Start_date DATETIME NOT NULL,
   End_date DATETIME NOT NULL,
   Start_odometer_value REAL NOT NULL,
   End_odometer_value REAL,
   Number_seats SMALLINT NOT NULL,
   License_number CHAR(18) NOT NULL,
   ID_city_official CHAR(8) NOT NULL,
   ID_vehicle INT NOT NULL,
   FOREIGN KEY(License_number) REFERENCES Driver(License_number),
   FOREIGN KEY(ID_city_official) REFERENCES Official(ID_city_official),
   FOREIGN KEY(ID_vehicle) REFERENCES Vehicle(ID_vehicle)
);

-- Create table Maintenance
CREATE TABLE Maintenance(
   Maintenance_date DATETIME,
   Type CHAR(1) NOT NULL,
   Odometer DOUBLE NOT NULL,
   Final_cost DOUBLE NOT NULL,
   Description TEXT NOT NULL,
   ID_vehicle INT NOT NULL,
   PRIMARY KEY(Maintenance_date),
   FOREIGN KEY(ID_vehicle) REFERENCES Vehicle(ID_vehicle)
   CONSTRAINT CHECK_Maintenance_Type CHECK (Type = 'M' OR Type = 'R')
);

-- Create table Has
CREATE TABLE Has(
   ID_country INT,
   Code CHAR(2),
   PRIMARY KEY(ID_country, Code),
   FOREIGN KEY(ID_country) REFERENCES Country(ID_country),
   FOREIGN KEY(Code) REFERENCES _Language(Code)
);

-- Create table Speaks
CREATE TABLE Speaks(
   Code CHAR(2),
   License_number CHAR(18),
   PRIMARY KEY(Code, License_number),
   FOREIGN KEY(Code) REFERENCES _Language(Code),
   FOREIGN KEY(License_number) REFERENCES Driver(License_number)
);

-- Checking if all tables exist
r_set = my_conn.execute('''select name from sqlite_master 
  where type = 'table' ''')
for row in r_set:
  print(row)

-- Overlapping vehicle trigger
CREATE TRIGGER IF NOT EXISTS TRIGGER_Booked_Vehicle_Slot_Is_Overlapping
BEFORE INSERT
ON Booking
FOR EACH ROW
    WHEN (SELECT COUNT(*)
        FROM Booking, Vehicle
        WHERE Booking.ID_vehicle = Vehicle.ID_vehicle
        AND Booking.ID_vehicle = NEW.ID_vehicle
        AND NEW.End_Date >= Booking.Start_Date
        AND NEW.Start_Date <= Booking.End_Date
        ) > 0
BEGIN
    SELECT RAISE(ABORT, 'This vehicle has already been booked for this slot of time');
END;

-- Overlapping driver trigger
CREATE TRIGGER IF NOT EXISTS TRIGGER_Booked_Driver_Slot_Is_Overlapping
BEFORE INSERT
ON Booking
FOR EACH ROW
    WHEN (SELECT COUNT(*)
        FROM Booking, Driver
        WHERE Booking.License_number = Driver.License_number
        AND Booking.License_number = NEW.License_number
        AND NEW.End_Date >= Booking.Start_Date
        AND NEW.Start_Date <= Booking.End_Date
        ) > 0
BEGIN
    SELECT RAISE(ABORT, 'This driver has already been booked for this slot of time');
END;

-- Overlapping official trigger
CREATE TRIGGER IF NOT EXISTS TRIGGER_Booked_Official_Slot_Is_Overlapping
BEFORE INSERT
ON Booking
FOR EACH ROW
    WHEN (SELECT COUNT(*)
        FROM Booking, Official
        WHERE Booking.ID_city_official = Official.ID_city_official
        AND Booking.ID_city_official = NEW.ID_city_official
        AND NEW.End_Date >= Booking.Start_Date
        AND NEW.Start_Date <= Booking.End_Date
        ) > 0
BEGIN
    SELECT RAISE(ABORT, 'This official has already been booked for this slot of time');
END;

-- Driver and official same language trigger
CREATE TRIGGER IF NOT EXISTS TRIGGER_Driver_And_Official_Have_Same_Language
BEFORE INSERT
ON Booking
FOR EACH ROW
    WHEN (SELECT COUNT(*)
        FROM Driver, Official, Speaks
        WHERE Official.ID_city_official = NEW.ID_city_official
        AND Driver.License_number = NEW.License_number
        AND Driver.License_number = Speaks.License_number
        AND Speaks.Code = Official.Code
        ) == 0
BEGIN
    SELECT RAISE(ABORT, 'The driver and the official do not speak the same language');
END;

-- Enough seats in the vehicle trigger
CREATE TRIGGER IF NOT EXISTS TRIGGER_Enough_Seats
BEFORE INSERT
ON Booking
FOR EACH ROW
    WHEN (SELECT COUNT(*)
        FROM Vehicle
        WHERE Vehicle.ID_vehicle = NEW.ID_vehicle
        AND NEW.Number_seats <= Vehicle.Passenger_capacity
        ) == 0
BEGIN
    SELECT RAISE(ABORT, 'There is not enough seats in this vehicle for this reservation');
END;

-- Inserting data into Vehicle
INSERT INTO Vehicle (ID_vehicle, Registration_id, Manufacturer, Model, Color, Current_odometer, Passenger_capacity, Unavailable) VALUES
(1, 'AB-123-CD', 'Volkswagen', 'Golf', 'Blue', 10000, 5, 0),
(2, 'AB-456-CD', 'Volvo', 'XC90 SE', 'Silver', 20000, 3, 0),
(3, 'AB-789-CD', 'Volkswagen', 'Golf', 'Green', 30000, 3, 0),
(4, 'AB-012-CD', 'Volkswagen', 'Golf', 'Yellow', 40000, 3, 0),
(5, 'AB-345-CD', 'Kia', 'K7', 'Black', 50000, 4, 0),
(6, 'AB-678-CD', 'Volkswagen', 'Golf', 'White', 60000, 3, 0),
(7, 'AB-901-CD', 'Tesla', '2020 F', 'White', 70000, 2, 1),
(8, 'AB-234-CD', 'Ford', 'Transit', 'Silver', 80000, 3, 1),
(9, 'AB-567-CD', 'Hyundai', 'i30', 'Green', 90000, 4, 0),
(10, 'AB-890-CD', 'Hyundai', 'i30', 'Yellow', 100000, 2, 0),
(11, 'AB-123-DE', 'Hyundai', 'i30', 'Black', 110000, 3, 0),
(12, 'AB-456-DE', 'Hyundai', 'i30', 'White', 120000, 1, 0);

-- Inserting data into Country
INSERT INTO Country (ID_country, Name) VALUES
(1, 'Belgium'),
(2, 'France'),
(3, 'Germany'),
(4, 'Italy'),
(5, 'Luxembourg'),
(6, 'Netherlands'),
(7, 'Spain');

-- Inserting data into _Language
INSERT INTO _Language(Code) VALUES
('EN'),
('FR'),
('DE'),
('IT'),
('NL'),
('ES'),
('LU'),
('BE'),
('PT');

-- Inserting data into Official
INSERT INTO Official(ID_city_official, Name, Role, ID_country, Code) VALUES
('12345678', 'John', 'Mayor', 1, 'EN'),
('23456789', 'Paul', 'Chief', 2, 'FR'),
('34567890', 'George', 'Supporter', 3, 'DE'),
('45678901', 'Ringo', 'Dresser', 4, 'IT'),
('56789012', 'Pete', 'Coach', 5, 'LU'),
('67890123', 'Stuart', 'Judge', 6, 'NL'),
('78901234', 'Mick', 'Physician', 7, 'ES');

-- Inserting data into Driver
INSERT INTO Driver(License_number, Name, Clearance_level, FATL_level, FATL_qualification_date,
    STLVT_level, STLVT_qualification_date, STLVT_certifying_authority) VALUES
('12345678901234567', 'Diego', 1, 2, '2018-01-01', 5, '2018-01-10', 'Foreign Police Station'),
('23456789012345678', 'Thierry', 2, 6, '2004-01-01', 4, '2004-02-11', 'Foreign Police Station'),
('34567890123456789', 'Sandra', 2, 4, '2010-03-12', 3, '2010-12-22', 'Foreign Police Station'),
('45678901234567890', 'Ines', 1, 9, '2015-02-14', 4, '2015-03-17', 'Police Station'),
('56789012345678901', 'Helene', 1, 10, '2018-09-17', 2, '2018-03-8', 'Police Station'),
('67890123456789012', 'Laura', 4, 6, '2020-07-28', 1, '2018-05-29', 'Police Station'),
('78901234567890123', 'Elena', 3, 7, '2012-11-10', 4, '2005 -02-02', 'Police Station'),
('89012345678901234', 'Luis', 2, 8, '2008-12-31', null, null, null),
('90123456789012345', 'Sofia', 2, null, null, null, null, null),
('01234567890123456', 'Luisa', 1, null, null, null, null, null);

-- Inserting data into Booking
INSERT INTO Booking(Booking_reference_number, Pick_up_location_name, Pick_up_location_street, Pick_up_location_street_number,
    Pick_up_location_city, Pick_up_location_type, Drop_off_location_name, Drop_off_location_street, Drop_off_location_street_number,
    Drop_off_location_city, Drop_off_location_type, Start_Date, End_Date, Start_odometer_value, End_odometer_value,
    Number_seats, License_number, ID_city_official, ID_vehicle) VALUES
(1, 'City Hall', 'Law street', 1, 'Incheon', 'City Hall', 'The Hotel', 'Hotel street', 5, 'Incheon', 'Hotel', '2020-01-01 10:00:00', '2020-01-01 12:00:00', 10000, 11000, 4, '12345678901234567', '12345678', 1),
(2, 'Swimming Pool', 'Pool street', 2, 'Incheon', 'Swimming Pool', 'The Garage', 'Garage street', 8, 'Incheon', 'Garage', '2020-01-02 08:00:00', '2020-01-02 10:00:00', 20000, 21000, 3, '23456789012345678', '23456789', 2),
(3, 'The Garage', 'Garage street', 12, 'Incheon', 'Garage', 'Gymnasium', 'Gym street', 3, 'Incheon', 'Gymnasium', '2020-01-02 10:00:01', '2020-01-02 12:00:00', 30000, 31000, 3, '12345678901234567', '23456789', 3),
(4, 'The Hotel', 'Hotel street', 5, 'Incheon', 'Hotel', 'The Hotel', 'Hotel other street', 9, 'Incheon', 'Hotel', '2020-01-02 14:00:00', '2020-01-02 15:00:00', 50000, 51000, 2, '12345678901234567', '12345678', 5);

-- Inserting data into Maintenance
INSERT INTO Maintenance(Maintenance_date, Type, Odometer, Final_cost, Description, ID_vehicle) VALUES
('2020-01-02 07:00:00', 'M', 10000, 340, 'Oil change', 1),
('2020-01-01 08:00:00', 'R', 20000, 150, 'Tire change', 2),
('2020-01-05 10:00:00', 'R', 31450, 420, 'Motor repair', 3),
('2020-01-03 11:00:00', 'M', 41430, 50, 'Liquid change', 4),
('2020-01-04 12:00:00', 'M', 52404, 100, 'Fix seats', 5);

-- Inserting data into Has
INSERT INTO HAS (ID_country, Code) VALUES
(1, 'EN'),
(2, 'FR'),
(3, 'DE'),
(4, 'IT'),
(5, 'LU'),
(6, 'NL'),
(7, 'ES'),
(1, 'FR'),
(2, 'EN'),
(3, 'FR'),
(4, 'FR'),
(5, 'FR'),
(6, 'FR'),
(7, 'FR'),
(1, 'DE');

-- Inserting data into Speaks
INSERT INTO SPEAKS (Code, License_number) VALUES
('EN', '12345678901234567'),
('FR', '12345678901234567'),
('DE', '12345678901234567'),
('IT', '12345678901234567'),
('NL', '12345678901234567'),
('ES', '12345678901234567'),
('LU', '12345678901234567'),
('BE', '12345678901234567'),
('PT', '12345678901234567'),
('EN', '23456789012345678'),
('FR', '23456789012345678'),
('DE', '23456789012345678');

-- Printing data of Vehicle
query = "SELECT * FROM Vehicle"
results=my_conn.execute(query)
for row in results:
  print(row)

-- Printing data of Country
query = "SELECT * FROM Country"
results=my_conn.execute(query)
for row in results:
  print(row)

-- Printing data of _Language
query = "SELECT * FROM _Language"
results=my_conn.execute(query)
for row in results:
  print(row)

-- Printing data of Official
query = "SELECT * FROM Official"
results=my_conn.execute(query)
for row in results:
  print(row)

-- Printing data of Driver
query = "SELECT * FROM Driver"
results=my_conn.execute(query)
for row in results:
  print(row)

-- Printing data of Booking
query = "SELECT * FROM Booking"
results=my_conn.execute(query)
for row in results:
  print(row)

-- Printing data of Maintenance
query = "SELECT * FROM Maintenance"
results=my_conn.execute(query)
for row in results:
  print(row)

-- Printing data of Has
query = "SELECT * FROM Has"
results=my_conn.execute(query)
for row in results:
  print(row)

-- Printing data of Speaks
query = "SELECT * FROM Speaks"
results=my_conn.execute(query)
for row in results:
  print(row)

-- Test TRIGGER_Booked_Vehicle_Slot_Is_Overlapping
my_conn.execute('''INSERT INTO Booking(Booking_reference_number, Pick_up_location_name, Pick_up_location_street, Pick_up_location_street_number,
    Pick_up_location_city, Pick_up_location_type, Drop_off_location_name, Drop_off_location_street, Drop_off_location_street_number,
    Drop_off_location_city, Drop_off_location_type, Start_Date, End_Date, Start_odometer_value, End_odometer_value,
    Number_seats, License_number, ID_city_official, ID_vehicle) VALUES
(5, 'Blabla', 'Bloblo', 1, 'Incheon', 'Blibli', 'Blublu', 'Bleble', 5, 'Incheon', 'Bou', '2020-01-01 10:00:00', '2020-01-01 12:00:00', 12000, 12034, 3, '23456789012345678', '23456789', 1);''')

-- Test TRIGGER_Booked_Driver_Slot_Is_Overlapping
my_conn.execute('''INSERT INTO Booking(Booking_reference_number, Pick_up_location_name, Pick_up_location_street, Pick_up_location_street_number,
    Pick_up_location_city, Pick_up_location_type, Drop_off_location_name, Drop_off_location_street, Drop_off_location_street_number,
    Drop_off_location_city, Drop_off_location_type, Start_Date, End_Date, Start_odometer_value, End_odometer_value,
    Number_seats, License_number, ID_city_official, ID_vehicle) VALUES
(5, 'Blabla', 'Bloblo', 1, 'Incheon', 'Blibli', 'Blublu', 'Bleble', 5, 'Incheon', 'Bou', '2020-01-01 10:00:00', '2020-01-01 12:00:00', 12000, 12034, 3, '12345678901234567', '23456789', 3);''')

-- Test TRIGGER_Booked_Official_Slot_Is_Overlapping
my_conn.execute('''INSERT INTO Booking(Booking_reference_number, Pick_up_location_name, Pick_up_location_street, Pick_up_location_street_number,
    Pick_up_location_city, Pick_up_location_type, Drop_off_location_name, Drop_off_location_street, Drop_off_location_street_number,
    Drop_off_location_city, Drop_off_location_type, Start_Date, End_Date, Start_odometer_value, End_odometer_value,
    Number_seats, License_number, ID_city_official, ID_vehicle) VALUES
(5, 'Blabla', 'Bloblo', 1, 'Incheon', 'Blibli', 'Blublu', 'Bleble', 5, 'Incheon', 'Bou', '2020-01-01 10:00:00', '2020-01-01 12:00:00', 12000, 12034, 3, '23456789012345678', '12345678', 3);''')

-- Test TRIGGER_Driver_And_Official_Have_Same_Language
my_conn.execute('''INSERT INTO Booking(Booking_reference_number, Pick_up_location_name, Pick_up_location_street, Pick_up_location_street_number,
    Pick_up_location_city, Pick_up_location_type, Drop_off_location_name, Drop_off_location_street, Drop_off_location_street_number,
    Drop_off_location_city, Drop_off_location_type, Start_Date, End_Date, Start_odometer_value, End_odometer_value,
    Number_seats, License_number, ID_city_official, ID_vehicle) VALUES
(5, 'Blabla', 'Bloblo', 1, 'Incheon', 'Blibli', 'Blublu', 'Bleble', 5, 'Incheon', 'Bou', '2020-01-01 10:00:00', '2020-01-04 12:00:00', 12000, 12034, 3, '34567890123456789', '12345678', 4);''')

-- Test TRIGGER_Enough_Seats
my_conn.execute('''INSERT INTO Booking(Booking_reference_number, Pick_up_location_name, Pick_up_location_street, Pick_up_location_street_number,
    Pick_up_location_city, Pick_up_location_type, Drop_off_location_name, Drop_off_location_street, Drop_off_location_street_number,
    Drop_off_location_city, Drop_off_location_type, Start_Date, End_Date, Start_odometer_value, End_odometer_value,
    Number_seats, License_number, ID_city_official, ID_vehicle) VALUES
(5, 'Blabla', 'Bloblo', 1, 'Incheon', 'Blibli', 'Blublu', 'Bleble', 5, 'Incheon', 'Bou', '2020-01-01 10:00:00', '2020-01-04 12:00:00', 12000, 12034, 10, '12345678901234567', '12345678', 4);''')

-- Test CHECK_Clearance_Level
my_conn.execute('''INSERT INTO Driver(License_number, Name, Clearance_level, FATL_level, FATL_qualification_date,
    STLVT_level, STLVT_qualification_date, STLVT_certifying_authority) VALUES
('94345638901234567', 'MyTest', 6, 2, '2018-01-01', 5, '2018-01-10', 'Foreign Police Station');''')

-- Test CHECK_FATL_Level
my_conn.execute('''INSERT INTO Driver(License_number, Name, Clearance_level, FATL_level, FATL_qualification_date,
    STLVT_level, STLVT_qualification_date, STLVT_certifying_authority) VALUES
('94345638901234567', 'MyTest', 1, 11, '2018-01-01', 5, '2018-01-10', 'Foreign Police Station');''')

-- Test CHECK_STLVT_Level
my_conn.execute('''INSERT INTO Driver(License_number, Name, Clearance_level, FATL_level, FATL_qualification_date,
    STLVT_level, STLVT_qualification_date, STLVT_certifying_authority) VALUES
('94345638901234567', 'MyTest', 1, 2, '2018-01-01', 8, '2018-01-10', 'Foreign Police Station');''')

-- Test CHECK_FATL_Information_Consistency
my_conn.execute('''INSERT INTO Driver(License_number, Name, Clearance_level, FATL_level, FATL_qualification_date,
    STLVT_level, STLVT_qualification_date, STLVT_certifying_authority) VALUES
('94345638901234567', 'MyTest', 1, null, '2018-01-01', 3, '2018-01-10', 'Foreign Police Station');''')

-- Test CHECK_STLVT_Information_Consistency
my_conn.execute('''INSERT INTO Driver(License_number, Name, Clearance_level, FATL_level, FATL_qualification_date,
    STLVT_level, STLVT_qualification_date, STLVT_certifying_authority) VALUES
('94345638901234567', 'MyTest', 1, 2, '2018-01-01', 3, null, 'Foreign Police Station');''')

-- Test CHECK_Maintenance_Type
my_conn.execute('''INSERT INTO Maintenance(Maintenance_date, Type, Odometer, Final_cost, Description, ID_vehicle) VALUES
('2020-01-05 10:00:00', 'V', 10000, 340, 'Oil change', 1);''')

-- Print all the reservations made by John the mayor
query = ('''SELECT Booking.* FROM Official, Booking
WHERE Official.ID_city_official = Booking.ID_city_official
AND Official.Name = 'John'
AND Official.Role = 'Mayor'; ''')
results=my_conn.execute(query)
for row in results:
  print(row)

-- Print every Official that prefers French
query = ('''SELECT Official.ID_city_official, Official.Name FROM Official
WHERE Official.Code = 'FR'; ''')
results=my_conn.execute(query)
for row in results:
  print(row)

-- Print cars that had a liquid change
query = ('''SELECT Vehicle.ID_vehicle, Vehicle.Registration_id FROM Maintenance, Vehicle
WHERE Vehicle.ID_vehicle = Maintenance.ID_vehicle
AND Maintenance.Description = 'Liquid change'; ''')
results=my_conn.execute(query)
for row in results:
  print(row)