CREATE TABLE IF NOT EXISTS Vehicle(
   ID_vehicle INTEGER PRIMARY KEY AUTOINCREMENT,
   Registration_id CHAR(8) NOT NULL,
   Manufacturer VARCHAR(25) NOT NULL,
   Model VARCHAR(30) NOT NULL,
   Color VARCHAR(15) NOT NULL,
   Current_odometer REAL NOT NULL,
   Passenger_capacity SMALLINT NOT NULL,
   PRIMARY KEY(ID_vehicle),
   UNIQUE(Registration_id)
);

CREATE TABLE IF NOT EXISTS Country(
   ID_country INTEGER PRIMARY KEY AUTOINCREMENT,
   Name VARCHAR(20) NOT NULL,
   PRIMARY KEY(ID_country)
);

CREATE TABLE IF NOT EXISTS _Language(
   Code CHAR(2),
   PRIMARY KEY(Code)
);

CREATE TABLE IF NOT EXISTS Official(
   ID_city_official CHAR(8),
   Name VARCHAR(20) NOT NULL,
   Role VARCHAR(30) NOT NULL,
   ID_country INT NOT NULL,
   Code CHAR(2) NOT NULL,
   PRIMARY KEY(ID_city_official),
   FOREIGN KEY(ID_country) REFERENCES Country(ID_country),
   FOREIGN KEY(Code) REFERENCES _Language(Code)
);

CREATE TABLE IF NOT EXISTS Driver(
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

CREATE TABLE IF NOT EXISTS Booking(
   Booking_reference_number INTEGER PRIMARY KEY AUTOINCREMENT,
   Start_date DATETIME NOT NULL,
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
   End_date DATETIME NOT NULL,
   Start_odometer_value REAL NOT NULL,
   End_odometer_value REAL,
   Number_seats SMALLINT NOT NULL,
   License_number CHAR(18) NOT NULL,
   ID_city_official CHAR(8) NOT NULL,
   ID_vehicle INT NOT NULL,
   PRIMARY KEY(Booking_reference_number),
   FOREIGN KEY(License_number) REFERENCES Driver(License_number),
   FOREIGN KEY(ID_city_official) REFERENCES Official(ID_city_official),
   FOREIGN KEY(ID_vehicle) REFERENCES Vehicle(ID_vehicle),
   CONSTRAINT CHECK_Enough_Seats CHECK (Number_seats <= ID_vehicle.Passenger_capacity)
);

CREATE TABLE IF NOT EXISTS Maintenance(
   Maintenance_date DATE,
   Maintenance_time TIME,
   Type CHAR(1) NOT NULL,
   Odometer DOUBLE NOT NULL,
   Final_cost DOUBLE NOT NULL,
   Description TEXT NOT NULL,
   ID_vehicle INT NOT NULL,
   PRIMARY KEY(Maintenance_date, Maintenance_time),
   FOREIGN KEY(ID_vehicle) REFERENCES Vehicle(ID_vehicle)
);

CREATE TABLE IF NOT EXISTS Has(
   ID_country INT,
   Code CHAR(2),
   PRIMARY KEY(ID_country, Code),
   FOREIGN KEY(ID_country) REFERENCES Country(ID_country),
   FOREIGN KEY(Code) REFERENCES _Language(Code)
);

CREATE TABLE IF NOT EXISTS Speaks(
   Code CHAR(2),
   License_number CHAR(18),
   PRIMARY KEY(Code, License_number),
   FOREIGN KEY(Code) REFERENCES _Language(Code),
   FOREIGN KEY(License_number) REFERENCES Driver(License_number)
);

CREATE TRIGGER IF NOT EXISTS TRIGGER_Booked_Vehicle_Slot_Is_Overlapping
BEFORE INSERT
ON Booking
FOR EACH ROW
    WHEN (SELECT COUNT(*)
        FROM Booking, Vehicle
        WHERE Booking.Vehicle_ID = Vehicle.Vehicle_ID
        AND Booking.Vehicle_ID = NEW.Vehicle_ID
        AND NEW.End_Date >= Booking.Start_Date
        AND NEW.Start_Date <= Booking.End_Date
        ) > 0
BEGIN
    SELECT RAISE(ABORT, 'This vehicle has already been booked for this slot of time');
END;

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