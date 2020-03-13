-- ASSUME 500+ ENTRIES REQUIRED FOR BTREE

-- 20000 Entries
CREATE INDEX flight_i
ON Flight
USING BTREE (fnum);

-- Customer ONLY has 250 entrees. NO BTREE
-- Pilot ONLY has 250 entrees. NO BTREE
-- Plane ONLY has 67 entrees. NO BTREE
-- Technician ONLY has 250 entrees. NO BTREE

-- 9999 Entries
CREATE INDEX reservation_i
ON Reservation
USING BTREE (rnum);

-- 2000 Entries
CREATE INDEX flightinfo_i
ON FlightInfo
USING BTREE (fiid);

-- 550 Entries
CREATE INDEX repairs_i
ON Repairs
USING BTREE (rid);

-- 2000 Entries
CREATE INDEX schedule_i
ON Schedule
USING BTREE (id);
